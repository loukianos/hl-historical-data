# Hyperliquid Historical Data Service

## Problem

The Sugon trading system needs a research data platform to analyze historical Hyperliquid exchange data. Researchers and automated systems need to query fill-level data by wallet, coin, and time range to study trader behavior, compute aggregations (VWAP, time bars), and build trading signals. Currently there is no structured way to access or query historical Hyperliquid fill data.

## Solution Overview

A Rust service (`hl-historical-data`) in a standalone repo that:
1. Ingests historical fill data from Hyperliquid's S3 archives into QuestDB
2. Serves queries via gRPC with server-side streaming
3. Provides built-in aggregation queries (VWAP, time bars, wallet summaries)
4. Includes a Python SDK (Polars-native) for notebook/research use

## Key Decisions

### Architecture
- **Single binary with subcommands**: `hl-historical-data serve`, `hl-historical-data backfill`, `hl-historical-data admin`
- **QuestDB** for time-series storage, deployed via Docker Compose alongside Redis
- **gRPC** for query API, consistent with Sugon's Protobuf-based inter-service communication
- **Proto definitions** defined in the Rust project
- **No authentication** — local-only service, binds to localhost
- **Minimal logging** — basic stdout logging, no Prometheus/Grafana integration
- **TOML config file** for all configuration
- **Same process model** — gRPC server spawns backfill as async Tokio tasks
- **Runs via Docker compose** - defined inside the crate
- **Successfully starts and stops multiple times without having to re-download all data** - Backfilled data is persisted on disk between sessions on the research machine, so that the service can be started, brought up-to-date (and only downloading all data since last sync), and have all data available
- **Runs Locally** - not intended to be deployed anywhere, but to be run on the local research machine, which has a 20TB HDD and 64GB of RAM

### Data Model: Fills (not Trades)
- Store fills in their native format, not trades. Each fill represents one side (or one part of one side) of a trade.
- Fills are more natural for trader-centric analysis (most queries are "what did wallet X do?")
- Trade reconstruction (joining multiple fills by `tid`) deferred — consumers can join by `tid` themselves
- **All coins ingested** (perps + spot) — no allowlist filtering at ingestion time
- Query-time coin filter available for scoping results

### Fill Schema (QuestDB Table: `fills`)
All fields from the `node_fills_by_block` format:
- `time` (designated timestamp, from fill's epoch ms `time` field) — QuestDB designated timestamp
- `block_time` (timestamp, from block envelope)
- `block_number` (int)
- `address` (string, 0x-prefixed wallet address from block envelope tuple)
- `coin` (string, e.g. "BTC", "ETH", "@107" for spot)
- `type` (string, "PERP" or "SPOT" or "OUTCOME")
- `px` (decimal, decimal price)
- `sz` (decimal, decimal size)
- `is_buy` (boolean, True when "side" is "B", False when "side" is "A")
- `start_position` (decimal, position before fill)
- `is_gaining_inventory` (boolean, describes if the user is gaining or losing inventory on a position e.g. False when start_position is negative and the fill is a buy (or open long in the case of perps), True when start_position is positive and fill is a buy or open long)
- `closed_pnl` (decimal, realized PnL)
- `hash` (string, 0x-prefixed tx hash)
- `oid` (long, order ID)
- `crossed` (boolean, true=taker/aggressive)
- `fee` (decimal, decimal fee amount, negative=rebate)
- `tid` (long, trade ID — both sides share same tid)
- `fee_token` (string, e.g. "USDC", "HYPE")
- `cloid` (string, optional client order ID)
- `builder_fee` (string, optional builder fee)
- `builder` (string, optional builder address)
- `local_time` (timestamp, node's local clock from envelope)

**Partitioning**: Daily partitions on `time`
**Quarantine table**: `fills_quarantine` for records that fail validation (price <= 0, size <= 0, unreasonable timestamps)

### Data Source: S3 Backfill Only (MVP)
- **Source**: `s3://hl-mainnet-node-data/node_fills_by_block/hourly/[YYYYMMDD]/[hour].lz4`
- **Format**: JSONL (one JSON object per line), LZ4 compressed
- **Range**: Data starts 2025-07-27, updated through present
- **File sizes**: ~1.5MB (quiet hours) to ~40MB (busy hours), compressed
- **Only `node_fills_by_block` format** — older formats (`node_fills`, `node_trades`) not supported
- **No live WebSocket streaming** for MVP

### Backfill Process
- **Manual trigger only** — no scheduled/daemon mode
- **`SyncToPresent`**: Backfill from last run to present, with 1-hour overlap for dedup
- **Resume**: Query `SELECT max(time) FROM fills` to determine resume point
- **Dedup**: Wipe-and-reinsert per hourly time range (`DELETE FROM fills WHERE time >= hour_start AND time < hour_end`)
- **Date-range partitioned**: Can specify `--from` and `--to` dates for targeted backfill
- **Single-threaded S3 downloads**: One file at a time
- **Decompress to disk**: Download LZ4 to temp file, decompress, then parse/ingest
- **ILP ingestion**: Use QuestDB's InfluxDB Line Protocol (port 9009) for high-throughput writes
- **Error handling**: Skip bad records/files, log warning, continue. Report skipped items in status.
- **JSONL parsing**: Lenient (ignore unknown fields) with warnings logged

### Validation
- Price > 0, size > 0, timestamp in reasonable range
- Valid fills → `fills` table
- Invalid fills → `fills_quarantine` table for review

### gRPC Query API

**Core RPCs** (all filter parameters optional except time range which is mandatory):

1. **`GetFills`** — Stream fills matching filters
   - Params: `coin` (optional), `wallet` (optional), `side` (optional), `crossed_only` (optional), `start_time` (required), `end_time` (required)
   - Returns: server-side stream of Fill messages
   - "Aggressive trades" = `crossed=true` filter

2. **`GetFillByHash`** — Look up fill by transaction hash
   - Params: `hash`
   - Returns: single Fill (or list if multiple fills in same tx)

3. **`GetFillsByOid`** — Look up fills by order ID
   - Params: `oid`
   - Returns: stream of Fill messages

4. **`GetVWAP`** — Volume-weighted average price over time
   - Params: `coin`, `interval` (1s, 5s, 30s, 1m, 5m, 15m, 30m, 1h, 4h, 1d), `start_time`, `end_time`
   - Returns: stream of VWAP data points

5. **`GetTimeBars`** — OHLCV bars with rich metadata
   - Params: `coin`, `interval` (1s, 5s, 30s, 1m, 5m, 15m, 30m, 1h, 4h, 1d), `start_time`, `end_time`
   - Returns: stream of bars with: open, high, low, close, base_volume, notional_volume, trade_count, buy_volume, sell_volume (aggressor-initiated)

6. **`GetWalletSummary`** — Wallet activity summary with per-coin breakdown
   - Params: `wallet`, `start_time`, `end_time`
   - Returns: total volume, fill count, unique coins, maker/taker ratio, first/last fill timestamp, plus per-coin breakdown of volume/count/maker-taker ratio

7. **`ListCoins`** — List all coins available in the database
   - Returns: list of coin symbols with fill counts

**Admin RPCs:**

8. **`GetIngestionStatus`** — Current backfill state
9. **`TriggerBackfill`** — Backfill specific date range (`from_date`, `to_date`)
10. **`SyncToPresent`** — Backfill from last ingested hour (with 1-hour overlap) to present
11. **`GetDbStats`** — Row counts, disk usage, partition info
12. **`PurgeData`** — Delete data before a given date (drops daily partitions)
13. **`ReIndex`** — Trigger re-indexing if needed

**Query behavior:**
- Time range is always required for `GetFills`
- No result limits or timeouts — local-only trusted service
- QuestDB unavailable → fail fast with clear error message
- On-the-fly aggregation computation (no pre-computed caches)

### QuestDB Connection
- **Writes**: ILP protocol (port 9009)
- **Reads**: PostgreSQL wire protocol (port 8812)

### QuestDB Deployment
- Added to `docker-compose.yml`
- Data volume mounted to 20TB HDD
- Auto-initialize tables on first service startup (create `fills` and `fills_quarantine` tables with partitioning if they don't exist)

### Python SDK (`src/clients/python/hl-historical-client/`)
- **Polars-native**: All queries return Polars DataFrames
- **Transparent streaming**: SDK collects all gRPC stream chunks into a single DataFrame
- **DataFrame-first API**: e.g., `client.get_fills(coin="BTC", start=..., end=...)` → Polars DataFrame
- **pip-installable**: Standard Python package with pyproject.toml
- **Generated from protos**: Use grpcio-tools to generate Python stubs from included protos

### Data Retention
- **Configurable TTL** in TOML config
- **Manual purge only** — purge via admin API `PurgeData` RPC
- Purge implemented via QuestDB `ALTER TABLE DROP PARTITION` on daily partitions
- No automatic deletion

### Configuration (TOML)
```toml
[questdb]
ilp_host = "localhost"
ilp_port = 9009
pg_host = "localhost"
pg_port = 8812

[grpc]
host = "127.0.0.1"
port = 50051

[backfill]
s3_bucket = "hl-mainnet-node-data"
s3_prefix = "node_fills_by_block/hourly"
temp_dir = "/tmp/hl-backfill"

[retention]
ttl_days = 0  # 0 = keep forever
```

## Edge Cases

- S3 files may be missing for certain hours (data gaps) — skip and log, don't fail
- LZ4 decompression failure — skip file, log error, continue with next hour
- Malformed JSONL lines — skip line, increment quarantine counter, continue
- Very large result sets (all fills for BTC over a year) — gRPC streaming handles this, no memory issues
- Spot assets with `@` prefix in coin name — ingested as-is, queryable
- Optional fields (`cloid`, `builder_fee`, `builder`) may be absent — handle as nullable
- QuestDB restart during backfill — backfill fails, re-run resumes from last checkpoint
- Concurrent backfill + query — same Tokio runtime, QuestDB handles concurrent reads/writes
- Re-running backfill for already-ingested dates — wipe time range, re-insert (idempotent)
- S3 requester-pays — AWS CLI configured, use `--request-payer requester`

## Input File Information

Bucket: s3://hl-mainnet-node-data/node_fills_by_block/hourly/[YYYYMMDD]/[hour].lz4
Availability: 2025-07-27 through today (up to date!)
Granularity: One file per hour, all coins combined
Format: NDJSON, LZ4 compressed. One line per block, each containing zero or more fill events:

```
  {
    "local_time": "2025-07-27T08:50:10.334741319",
    "block_time": "2025-07-27T08:50:10.273720809",
    "block_number": 676607012,
    "events": [
      [
        "0x7839e2f2c375dd29...",       // user address
        {                               // fill object
          "coin": "BTC",
          "px": "118136.0",
          "sz": "0.00009",
          "side": "B",
          "time": 1753606210273,
          "startPosition": "-1.41864",
          "dir": "Close Short",
          "closedPnl": "-0.003753",
          "hash": "0xe782204...",
          "oid": 121670079265,
          "crossed": false,
          "fee": "-0.000212",
          "tid": 161270588369408,
          "feeToken": "USDC",
          // optional fields:
          "cloid": "0x09367b...",       // client order ID (if present)
          "builderFee": "0.005528",     // builder fee (if present)
          "builder": "0x49ae63..."      // builder address (if present)
        }
      ],
      ...  // more [address, fill] pairs
    ]
  }
```


## Open Questions

- Exact decompressed JSONL line format needs to be validated against a real S3 file during implementation
- QuestDB ILP Rust client library selection (questdb-rs, custom, etc.)
- Optimal QuestDB memory/config tuning for the 64GB RAM machine
- Whether sub-second time bars (1s, 5s, 30s) perform well enough via QuestDB SAMPLE BY or need custom aggregation logic

## Reviewer questions answered (plan + rationale)

### Should it run via Docker Compose?

Yes — use Docker Compose to run **QuestDB (required)** and **Redis (optional/placeholder)**, with QuestDB’s storage **mounted to a host directory on the 20TB HDD** so data persists between sessions.

Recommended dev ergonomics:

* `docker compose up -d questdb redis` brings up dependencies only
* run the Rust binary on the host (fast rebuild/debug):
  `cargo run --release -- serve --config ./config.toml`
* optionally also provide a `service` Compose target/profile later, but **not required** for MVP

### How to ensure historical data persists between sessions and you don’t re-download everything

1. **QuestDB persistence:** mount QuestDB’s data dir to a host path, e.g. `/mnt/hdd/hl/questdb` (or `./.data/questdb` by default).
2. **Resume logic:** `SyncToPresent` queries `SELECT max(time) FROM fills` to find the last ingested timestamp, then starts from `(that_hour - 1 hour overlap)` to now.
3. **Idempotent hourly ingestion:** for each hour, do:

   * `DELETE FROM fills WHERE time >= hour_start AND time < hour_end`
   * re-insert rows for that hour
4. **No need to persist downloads:** you *can* always re-download only the missing hours. (Optionally, you may cache decompressed files later, but it’s not necessary to meet the spec.)

This combination guarantees:

* service can start/stop repeatedly
* historical data remains available
* `--sync` downloads only new hours + 1-hour overlap (dedup window)

---

## Open questions resolved into a sensible implementation approach

### 3) “Optimal QuestDB memory/config tuning”

Plan:

* Ship Compose defaults that work out of the box.
* Allow overrides via env vars / compose configuration and document a “64GB RAM tuning” section.
* Treat tuning as a **separate issue** (non-blocking for correctness).

### 4) “Whether sub-second time bars perform well enough…”

Plan:

* Implement all intervals using QuestDB SQL `SAMPLE BY` first (simple, fast enough for MVP).
* Add one performance benchmark issue:

  * if `1s/5s/30s` is too slow, add an optional Rust-side aggregator fallback (only for these intervals) behind a config flag.

## Acceptance

- [ ] QuestDB running via Docker Compose with data on HDD
- [ ] `hl-historical-data backfill --from YYYYMMDD --to YYYYMMDD` downloads and ingests S3 data
- [ ] `hl-historical-data backfill --sync` resumes from last ingested hour with 1-hour overlap
- [ ] Backfill handles errors gracefully (skip + log + continue)
- [ ] Invalid fills routed to quarantine table
- [ ] `hl-historical-data serve` starts gRPC server on localhost
- [ ] GetFills query returns fills filtered by wallet, coin, time range, side, crossed
- [ ] GetFillByHash and GetFillsByOid return correct results
- [ ] GetVWAP returns VWAP at configured intervals
- [ ] GetTimeBars returns OHLCV bars with trade count and buy/sell volume, including sub-second intervals
- [ ] GetWalletSummary returns per-coin breakdown with volume, count, maker/taker ratio
- [ ] ListCoins returns available coins
- [ ] Admin RPCs work (ingestion status, trigger backfill, DB stats, purge, sync-to-present)
- [ ] Python SDK returns Polars DataFrames for all query types
- [ ] Python SDK installable via pip
- [ ] Configuration via TOML file
- [ ] Auto-creates QuestDB tables on first startup
