## Milestone 0 — Repo + local runtime foundation

### Issue 1 — Create repo scaffold + Rust workspace layout

**Goal:** Standalone repo `hl-historical-data` with consistent structure.

**Tasks**

* Create Cargo project with modules:

  * `src/main.rs` (CLI entry)
  * `src/config.rs` (TOML config)
  * `src/db/{mod.rs,questdb.rs}` (read/write)
  * `src/backfill/{mod.rs,s3.rs,parse.rs,ingest.rs,status.rs}`
  * `src/grpc/{mod.rs,server.rs,queries.rs,admin.rs}`
* Add `rustfmt`, `clippy` configs.
* Add `README.md` placeholder with quickstart steps.

**Acceptance criteria**

* `cargo build` succeeds
* `cargo fmt --check` and `cargo clippy` run clean (or with explicitly justified lints)

---

### Issue 2 — Add Docker Compose for QuestDB (persistent) + Redis (optional)

**Goal:** Dependencies run locally with persisted QuestDB data.

**Tasks**

* Add `docker-compose.yml` with at least:

  * `questdb` service exposing:

    * ILP TCP port `9009`
    * PG wire port `8812`
    * Web console port (QuestDB default) for debugging
  * `redis` service (even if unused for MVP; keep it minimal)
* Mount QuestDB storage to host path:

  * default: `./.data/questdb` inside repo
  * allow override via env: `HL_DATA_DIR=/mnt/hdd/hl`
* Add `.gitignore` entries for `.data/`
* Add `Makefile` (or scripts) shortcuts:

  * `make up`, `make down`, `make logs`, `make reset` (reset should be explicit and destructive)

**Acceptance criteria**

* `docker compose up -d questdb` starts successfully
* QuestDB data persists:

  * create a table manually via console, restart container, table still exists

---

### Issue 3 — Implement CLI skeleton with subcommands: `serve`, `backfill`, `admin`

**Goal:** Match spec’s process model.

**Tasks**

* Use `clap` to implement:

  * `hl-historical-data serve --config <path>`
  * `hl-historical-data backfill --from YYYYMMDD --to YYYYMMDD --config <path>`
  * `hl-historical-data backfill --sync --config <path>`
  * `hl-historical-data admin ...` (will call admin RPCs later)
* Add `--log-level` or use `RUST_LOG`.

**Acceptance criteria**

* `hl-historical-data --help` shows all subcommands/options
* Running each subcommand without required args yields clear CLI error messages

---

### Issue 4 — TOML config loader + defaults

**Goal:** Central configuration matches spec.

**Tasks**

* Implement TOML structs for:

  * `[questdb]` (hosts/ports)
  * `[grpc]` (host/port)
  * `[backfill]` (bucket/prefix/temp_dir)
  * `[retention]` (ttl_days)
* Support `--config` path; fail fast if missing/unreadable.
* Add `config.example.toml` matching the spec.

**Acceptance criteria**

* Service starts with `--config config.example.toml`
* Bad config surfaces a helpful error (which key is invalid/missing)

---

## Milestone 1 — Database schema + ingestion prerequisites

### Issue 5 — QuestDB schema creation on startup (fills + quarantine)

**Goal:** Auto-create `fills` and `fills_quarantine` if missing, with daily partitions.

**Tasks**

* On `serve` startup (and optionally on `backfill` start), connect to QuestDB via PG wire and run:

  * `CREATE TABLE IF NOT EXISTS fills (...) TIMESTAMP(time) PARTITION BY DAY;`
  * `CREATE TABLE IF NOT EXISTS fills_quarantine (...) TIMESTAMP(time) PARTITION BY DAY;`
* Add `fills_quarantine.reason` (string/symbol) to store why it was quarantined.
* Decide column types (document them in code):

  * timestamps: `TIMESTAMP`
  * numbers: likely `DOUBLE` for px/sz/etc unless QuestDB `DECIMAL` is confirmed
  * ids: `LONG`
  * booleans: `BOOLEAN`
  * strings: `VARCHAR` or `SYMBOL` (see Issue 6)
* Add indexes where appropriate (likely `coin`, optionally `address`).

**Acceptance criteria**

* Fresh QuestDB + `hl-historical-data serve` results in both tables existing
* Tables show daily partitioning on `time`

---

### Issue 6 — Decide and implement QuestDB column types + indexing strategy

**Goal:** Avoid “oops we symbolized a billion hashes” problems.

**Tasks**

* Make a deliberate choice for each high-cardinality field:

  * `address`: likely `SYMBOL` (query-heavy) but document memory implications
  * `hash`: likely `VARCHAR` (avoid symbol explosion) unless you add a separate hash lookup structure
  * `cloid`: probably `VARCHAR`
  * `coin`, `type`, `fee_token`: `SYMBOL`
* Add indexes:

  * `coin` indexed (low cardinality)
  * `address` indexed if `SYMBOL` (wallet queries are core)
* Add a short ADR in `docs/adr/0001-questdb-types.md`.

**Acceptance criteria**

* ADR exists explaining choices + tradeoffs
* Schema matches ADR
* A sample query by wallet and coin shows index usage benefits (documented qualitatively)

---

### Issue 7 — Implement QuestDB read client (PG wire) with query helpers

**Goal:** Reliable reads for gRPC queries and admin ops.

**Tasks**

* Use `tokio-postgres` for async querying.
* Provide helpers:

  * `query_stream(sql, params) -> RowStream` (or equivalent)
  * `execute(sql, params)` for DELETE / DDL
* Ensure reconnect logic is clear:

  * for MVP, “fail fast” is acceptable; return Unavailable on RPC calls if DB is down

**Acceptance criteria**

* A small `db ping` function works
* Running `SELECT 1` via client succeeds against local QuestDB

---

### Issue 8 — Implement QuestDB ILP write client (TCP line protocol)

**Goal:** High-throughput inserts.

**Tasks**

* Implement ILP writer:

  * connect to `ilp_host:ilp_port` (TCP)
  * write batches of ILP lines
  * flush per batch; handle broken pipe by reconnecting once
* Add batching controls:

  * batch size by lines or bytes (configurable constants)
* Provide unit tests for ILP line formatting (no network)

**Acceptance criteria**

* A simple “insert 1 row” test program writes into `fills` via ILP and row is visible via PG query

---

## Milestone 2 — Backfill engine (S3 → LZ4 → JSONL → validation → ILP)

### Issue 9 — Implement S3 hourly key generation + date range iteration

**Goal:** Deterministic mapping: `[YYYYMMDD]/[hour].lz4` for each hour.

**Tasks**

* Parse `--from YYYYMMDD`, `--to YYYYMMDD` inclusive (document semantics).
* Generate hourly timestamps across the range:

  * define whether `--to` includes the full day through 23:00 (recommended)
* Build S3 key: `{prefix}/hourly/{YYYYMMDD}/{HH}.lz4`

**Acceptance criteria**

* Unit tests: given from/to, produced list of keys matches expected
* Handles DST/timezones by using **UTC** consistently for keys/hours

---

### Issue 10 — Implement S3 downloader with requester-pays support

**Goal:** Download one hourly `.lz4` file robustly.

**Tasks**

* Use AWS SDK for Rust
* Support requester-pays:

  * set `RequestPayer::Requester` on requests
* Respect standard AWS credential resolution (env/credentials file/instance role)
* Error handling:

  * missing key → log warn and continue
  * access denied / auth errors → fail the backfill (actionable message)

**Acceptance criteria**

* With valid AWS creds, can download a known hour to `temp_dir`
* Missing hour logs warning and continues

---

### Issue 11 — Implement LZ4 decompression to disk (stream-friendly)

**Goal:** Decompress `.lz4` to a `.jsonl` file without loading whole file into memory.

**Tasks**

* Download to `temp_dir/<key>.lz4`
* Decompress to `temp_dir/<key>.jsonl`
* Ensure temp files cleaned up on success (or configurable “keep temp for debugging” flag)

**Acceptance criteria**

* Decompression failure is caught, logged, and backfill proceeds to next hour
* Large file does not cause high RSS memory spikes

---

### Issue 12 — Implement JSONL parsing for `node_fills_by_block` envelope

**Goal:** Parse each line into 0..N fill rows.

**Tasks**

* Parse top-level object:

  * `local_time` (string, ns precision)
  * `block_time` (string, ns precision)
  * `block_number` (int)
  * `events`: list of `[address, fill_object]`
* Parse fill object fields:

  * required: `coin`, `px`, `sz`, `side`, `time`, `startPosition`, `closedPnl`, `hash`, `oid`, `crossed`, `tid`, `fee`, `feeToken`
  * optional: `cloid`, `builderFee`, `builder`
* Ignore unknown fields but count/log them (rate-limited logging).

**Acceptance criteria**

* Unit test: one fixture JSON line produces expected number of fills
* Missing optional fields does not fail parsing

---

### Issue 13 — Implement derived fields: `is_buy`, `type`, `is_gaining_inventory`

**Goal:** Populate schema fields not directly present.

**Tasks**

* `is_buy = (side == "B")`
* `type`:

  * if fill has an explicit `type`, use it
  * else derive:

    * if `coin` starts with `@` → `SPOT`
    * else default to `PERP` (document; adjust if real data shows otherwise)
* `is_gaining_inventory`:

  * compute `result_position = start_position + (is_buy ? sz : -sz)`
  * `is_gaining_inventory = abs(result_position) > abs(start_position)` (use small epsilon)

**Acceptance criteria**

* Unit tests cover:

  * B/A side mapping
  * spot derivation with `@`
  * gaining/losing inventory examples

---

### Issue 14 — Implement validation + quarantine routing

**Goal:** Valid rows go to `fills`; invalid go to `fills_quarantine` with reason.

**Tasks**

* Validate:

  * `px > 0`
  * `sz > 0`
  * `time` within reasonable bounds:

    * not before 2025-07-27 (minus small skew)
    * not more than e.g. 24h in the future
* For invalid rows:

  * insert into `fills_quarantine`
  * include `reason` (“px<=0”, “sz<=0”, “bad_time”, “parse_error”, etc.)

**Acceptance criteria**

* Injecting known-bad rows results in quarantine inserts
* Backfill continues even with malformed lines

---

### Issue 15 — Implement hourly dedup: delete + reinsert within hour window

**Goal:** Idempotent backfill per hour.

**Tasks**

* For each hour `[hour_start, hour_end)`:

  * run `DELETE FROM fills WHERE time >= $1 AND time < $2`
  * ingest all valid rows for that hour
  * (optional) also delete quarantine rows in that hour to keep idempotent behavior consistent
* Ensure delete occurs **before** ingest to avoid duplicates

**Acceptance criteria**

* Running backfill twice for the same hour results in identical `count(*)` for that hour (no duplication)

---

### Issue 16 — Implement `backfill --from/--to` end-to-end

**Goal:** One command downloads, parses, validates, ingests.

**Tasks**

* Glue Issues 9–15 into the `backfill` subcommand
* Print a final summary:

  * hours processed / skipped
  * rows inserted
  * rows quarantined
  * files missing / failed decompress / parse errors

**Acceptance criteria**

* Command completes successfully for a small date range
* Summary matches observed DB counts

---

### Issue 17 — Implement `backfill --sync` (SyncToPresent) resume behavior

**Goal:** Only ingest new data since last run with 1-hour overlap.

**Tasks**

* Query QuestDB: `SELECT max(time) FROM fills`
* Compute resume hour start:

  * if null → start from `2025-07-27T00:00Z` (or require `--from` on first run; document)
  * else → truncate to hour and subtract 1 hour
* Determine “present” as current UTC hour (or up to last complete hour for stability)
* Backfill that computed range

**Acceptance criteria**

* First run ingests from configured start
* Second run shortly after processes at most ~2 hours (overlap + newest hour)

---

## Milestone 3 — gRPC API (streaming queries + aggregations)

### Issue 18 — Define protobufs for data + RPCs (core + admin)

**Goal:** Protos live in Rust project and generate Rust + Python stubs.

**Tasks**

* Create `proto/hl_historical.proto` with:

  * `Fill` message matching schema (nullable fields as optional / wrapper types)
  * request/response messages for all RPCs
  * enums:

    * `Side` (BUY/SELL/UNSPECIFIED)
    * `Interval` (S1, S5, S30, M1, M5, M15, M30, H1, H4, D1)
* Define services:

  * `HistoricalDataService` (core queries)
  * `HistoricalDataAdminService` (admin RPCs)
* Generate Rust via `tonic-build`

**Acceptance criteria**

* `cargo build` generates Rust gRPC code
* Proto compilation is reproducible from a clean checkout

---

### Issue 19 — Implement gRPC server bootstrap (`serve`)

**Goal:** `hl-historical-data serve` runs gRPC on localhost and connects to QuestDB.

**Tasks**

* Bind to `grpc.host:grpc.port` (default 127.0.0.1)
* On startup:

  * validate QuestDB connectivity
  * ensure tables exist (Issue 5)
* Implement graceful shutdown on SIGINT/SIGTERM.

**Acceptance criteria**

* `serve` starts and listens on configured port
* If QuestDB is down, service exits with clear error

---

### Issue 20 — Implement `GetFills` streaming query with filters

**Goal:** Stream fills for a time range + optional filters.

**Tasks**

* Enforce required fields: `start_time`, `end_time`
* Build SQL with parameters:

  * always constrain `time >= start AND time < end`
  * optional:

    * `coin = $n`
    * `address = $n`
    * `is_buy = true/false`
    * `crossed = true` if `crossed_only`
* Stream results:

  * Do not buffer all rows in memory
  * Map DB rows → proto `Fill`
* Order by `time ASC`.

**Acceptance criteria**

* Query returns correct subset by wallet/coin/side/crossed
* Streaming works on large ranges without memory blowups (spot-check via RSS)

---

### Issue 21 — Implement `GetFillByHash` and `GetFillsByOid`

**Goal:** Hash lookup and order-id lookup.

**Tasks**

* `GetFillByHash(hash)`:

  * query `WHERE hash = $1`
  * return repeated fills (since multiple fills per tx can exist)
* `GetFillsByOid(oid)`:

  * query `WHERE oid = $1 ORDER BY time ASC`
  * server-side streaming response

**Acceptance criteria**

* Both RPCs return correct results for known data
* Hash RPC returns >1 fill when tx contains multiple fills

---

### Issue 22 — Implement `ListCoins`

**Goal:** List coins present, with counts.

**Tasks**

* Query: `SELECT coin, count() AS fill_count FROM fills GROUP BY coin ORDER BY fill_count DESC`
* Return list of (coin, count)

**Acceptance criteria**

* Contains spot coins with `@` prefix as-is
* Counts match direct SQL spot-check

---

### Issue 23 — Implement `GetVWAP` aggregation

**Goal:** Stream VWAP points at configured interval.

**Tasks**

* Validate `coin`, `interval`, and time range
* SQL (conceptually):

  * bucket by interval using QuestDB sampling
  * compute:

    * `notional = sum(px * sz)`
    * `volume = sum(sz)`
    * `vwap = notional / volume` (guard div-by-zero)
    * `fill_count = count()`
* Stream points ordered by bucket time.

**Acceptance criteria**

* Returned timestamps align to interval buckets
* VWAP matches a manual calculation for a small sample window

---

### Issue 24 — Implement `GetTimeBars` (OHLCV + metadata)

**Goal:** Return bars with open/high/low/close, volumes, counts, buy/sell aggressor volumes.

**Tasks**

* For each bucket:

  * `open = first(px)`
  * `high = max(px)`
  * `low = min(px)`
  * `close = last(px)`
  * `base_volume = sum(sz)`
  * `notional_volume = sum(px*sz)`
  * `trade_count = count()`
  * `buy_volume = sum(sz) WHERE crossed=true AND is_buy=true`
  * `sell_volume = sum(sz) WHERE crossed=true AND is_buy=false`
* Ensure sub-second intervals work (1s/5s/30s) using `SAMPLE BY` initially.
* Add a benchmark harness (can be a separate binary) to evaluate performance later.

**Acceptance criteria**

* Bars returned for all supported intervals including 1s/5s/30s
* Buy/sell volumes reflect **aggressor-initiated** (crossed=true) logic

---

### Issue 25 — Implement `GetWalletSummary` (totals + per-coin breakdown)

**Goal:** Wallet activity summary over time range.

**Tasks**

* Compute totals for wallet:

  * fill_count
  * unique_coins
  * first_fill_time / last_fill_time
  * maker_count / taker_count and maker_taker_ratio
  * total notional and/or base volume (be explicit in proto field names)
* Compute per-coin breakdown with same metrics (group by coin).
* Return single response containing totals + repeated per-coin entries.

**Acceptance criteria**

* Totals match aggregating the per-coin rows
* Maker/taker ratio matches crossed=true/false counts

---

## Milestone 4 — Admin RPCs + operational controls

### Issue 26 — Implement ingestion job state tracking (`GetIngestionStatus`)

**Goal:** Visibility into current/last backfill run.

**Tasks**

* Maintain an in-memory shared `BackfillStatus`:

  * state: idle/running/failed/succeeded
  * started_at, last_updated_at, finished_at
  * current_hour, hours_done/total
  * rows_inserted, rows_quarantined
  * files_missing, files_failed
* Expose via admin RPC `GetIngestionStatus`

**Acceptance criteria**

* While backfill runs, status updates reflect progress
* After completion, status shows final counters

---

### Issue 27 — Implement admin-triggered backfills (`TriggerBackfill`, `SyncToPresent`)

**Goal:** Run backfill from within the server process.

**Tasks**

* `TriggerBackfill(from_date, to_date)`:

  * spawn tokio task
  * update status
  * prevent concurrent backfills (mutex/lock); return error if one is running
* `SyncToPresent()`:

  * same, but uses resume logic

**Acceptance criteria**

* Triggering backfill via gRPC starts work and updates status
* Second trigger while running returns a clear “already running” error

---

### Issue 28 — Implement `GetDbStats` (row counts + partition info)

**Goal:** Lightweight DB introspection.

**Tasks**

* Return:

  * total fills row count
  * total quarantine row count
  * min/max `time` in fills
  * list of partitions (date + row count) if QuestDB supports partition metadata queries
  * (optional) disk usage if a `data_dir` is configured (stat filesystem)
* Keep it fast (avoid scanning whole table repeatedly).

**Acceptance criteria**

* Response returns within a couple seconds on moderate datasets
* Counts match spot-check SQL queries

---

### Issue 29 — Implement `PurgeData` (drop partitions before a date)

**Goal:** Manual retention management.

**Tasks**

* Given `before_date`:

  * compute all daily partitions strictly older than that date
  * execute QuestDB `ALTER TABLE ... DROP PARTITION ...` for each
* Do this for both `fills` and `fills_quarantine`.
* Make it idempotent and safe:

  * if partition missing, continue

**Acceptance criteria**

* After purge, queries for old dates return 0 rows
* Disk usage decreases after QuestDB compaction/cleanup (document expectation)

---

### Issue 30 — Implement `ReIndex` as “ensure indexes exist / rebuild if missing”

**Goal:** Provide the admin hook without risky magic.

**Tasks**

* Define which indexes the system expects (from Issue 6 ADR).
* Implement `ReIndex` to:

  * check current indexed columns (via QuestDB metadata tables/functions)
  * add missing indexes (and only missing)
* Document that “true rebuild” may require manual steps; don’t drop indexes automatically in MVP.

**Acceptance criteria**

* If an index is missing, calling `ReIndex` creates it
* If all indexes exist, calling `ReIndex` is a no-op

---

### Issue 31 — Implement `admin` CLI wrapper commands

**Goal:** Everything can be operated from terminal without writing a client.

**Tasks**

* `hl-historical-data admin status`
* `hl-historical-data admin sync`
* `hl-historical-data admin backfill --from --to`
* `hl-historical-data admin stats`
* `hl-historical-data admin purge --before YYYYMMDD`
* `hl-historical-data admin reindex`
* Under the hood: connect to gRPC admin service.

**Acceptance criteria**

* Each command prints a human-readable response
* Commands fail gracefully if server isn’t running

---

## Milestone 5 — Python SDK (Polars-native)

### Issue 32 — Python package scaffold + proto generation

**Goal:** `pip install -e .` works; stubs generated from included protos.

**Tasks**

* Create `src/clients/python/hl-historical-client/`
* Add `pyproject.toml` with dependencies:

  * `grpcio`, `grpcio-tools`, `protobuf`, `polars`
* Add build step/script to generate Python code from `proto/hl_historical.proto` into package
* Ensure generated code is included in sdist/wheel (or generated at install time—pick one and document)

**Acceptance criteria**

* `pip install -e src/clients/python/hl-historical-client` succeeds
* `python -c "import hl_historical_client"` succeeds

---

### Issue 33 — Python client API returning Polars DataFrames

**Goal:** Notebook-friendly interface.

**Tasks**

* Implement `Client` class:

  * connects to gRPC server
  * methods:

    * `get_fills(...) -> pl.DataFrame`
    * `get_fills_by_oid(...) -> pl.DataFrame`
    * `get_fill_by_hash(...) -> pl.DataFrame` (or list, but DataFrame is simplest)
    * `get_vwap(...) -> pl.DataFrame`
    * `get_time_bars(...) -> pl.DataFrame`
    * `get_wallet_summary(...) -> (pl.DataFrame totals, pl.DataFrame per_coin)` **or** a dict with DataFrames
    * `list_coins() -> pl.DataFrame`
* Implement streaming collection:

  * iterate stream → list of dicts/tuples → `pl.DataFrame`
* Convert timestamps to Polars datetime type.

**Acceptance criteria**

* Each method returns a DataFrame with expected columns and dtypes
* Works against a locally-running server with real data

---

### Issue 34 — Python SDK examples (notebooks/scripts) + minimal docs

**Goal:** Researchers can use it immediately.

**Tasks**

* Add `examples/`:

  * `basic_fills_query.py`
  * `vwap_and_bars.py`
  * `wallet_summary.py`
* Add README in Python package showing usage + connection details.

**Acceptance criteria**

* Each example runs end-to-end against local server

---

## Milestone 6 — Reliability, testing, docs

### Issue 35 — End-to-end integration test with QuestDB (local fixture)

**Goal:** Catch regressions without hitting S3.

**Tasks**

* Add `testdata/` JSONL fixture representing a few blocks and fills.
* Add a test mode that ingests from a local file (internal-only hook is fine) so CI doesn’t need AWS.
* Spin up QuestDB in tests (docker compose in CI or a documented local step).
* Validate:

  * rows inserted
  * `GetFills` returns expected results
  * VWAP / bars match expected outputs for fixture

**Acceptance criteria**

* One command runs integration tests locally and in CI
* No AWS required for CI

---

### Issue 36 — Operational documentation: Quickstart + troubleshooting

**Goal:** Someone new can run it without tribal knowledge.

**Tasks**

* Expand root `README.md`:

  * prerequisites (Docker, Rust, AWS creds)
  * bring up QuestDB
  * first-time backfill
  * sync-to-present workflow
  * how to query via Python SDK
  * where data lives on disk and how to move it
  * common errors (requester-pays, missing hours, decompression failures)
* Document the “persisted on disk” guarantee explicitly.

**Acceptance criteria**

* A new engineer can follow README to get from zero → querying data locally

---

## Notes on “Redis alongside QuestDB”

The spec mentions Redis but doesn’t define any concrete use in MVP. To keep momentum while still matching the “alongside Redis” note:

* include Redis in Compose (Issue 2)
* do not block MVP on using it
* if later needed, Redis can store backfill status or caching — but for now, QuestDB + in-memory status satisfies the spec.

---

If you want, I can also provide a suggested **issue dependency graph** (what must land before what) and a **small initial proto schema draft**, but the backlog above is already complete enough to execute end-to-end without additional design work.
