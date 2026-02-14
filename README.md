# hl-historical-data

Standalone Rust service for ingesting Hyperliquid historical fills into QuestDB and querying them over gRPC, with a Polars-native Python SDK for research workflows.

> Designed for local/trusted use. By default the server binds to `127.0.0.1:50051` and does not use TLS/auth.

## Prerequisites

- Docker + Docker Compose v2
- Rust toolchain (`cargo`)
- AWS credentials with requester-pays access to `s3://hl-mainnet-node-data/...`
- Optional: Python 3 (for SDK/examples/notebooks)

## Ports (default)

- QuestDB Web Console: `http://localhost:9000`
- QuestDB ILP (writes): `localhost:9009`
- QuestDB PostgreSQL wire (reads): `localhost:8812`
- gRPC server: `127.0.0.1:50051`

## Quickstart (zero → first query)

All commands run from repo root.

1. Start dependencies (QuestDB + Redis):

```bash
make up
```

If you only want QuestDB:

```bash
docker compose up -d questdb
```

2. Create local config:

```bash
cp config.example.toml config.toml
```

3. Start the service:

```bash
cargo run -- serve --config config.toml
```

4. First-time backfill (small day range):

```bash
cargo run -- backfill --from 20250727 --to 20250727 --config config.toml
```

5. Ongoing sync-to-present workflow:

```bash
cargo run -- backfill --sync --config config.toml
```

`--sync` resumes from `max(time)` in `fills`, applies a 1-hour overlap for dedup, and clamps to the first available hour (`2025-07-27T00:00:00Z`).

6. Query via Python SDK:

```bash
bash scripts/setup_python_research_env.sh --with-jupyter
source .venv/bin/activate
python src/clients/python/hl-historical-client/examples/basic_fills_query.py --hours 1 --coin BTC
```

See:
- `src/clients/python/hl-historical-client/README.md`
- `docs/python-research-workflow.md`

## Data persistence guarantee (important)

QuestDB data is persisted on disk via Docker bind mounts in `docker-compose.yml`:

- Default path: `./.data/questdb`
- With `HL_DATA_DIR=/some/path`: `${HL_DATA_DIR}/questdb`

This means stop/start cycles (`make down`, `make up`) do **not** require re-downloading data.

Redis is also mounted (`./.data/redis` or `${HL_DATA_DIR}/redis`) but currently unused by the Rust service.

### Resetting data (destructive)

```bash
make reset
```

`make reset` deletes local `./.data/` after `docker compose down -v`.
If you use `HL_DATA_DIR`, remove `${HL_DATA_DIR}/questdb` yourself when you want a full wipe.

### Moving data to another disk/path

1. Stop containers:
   ```bash
   make down
   ```
2. Copy the `questdb/` directory to the new location.
3. Restart with new mount root:
   ```bash
   HL_DATA_DIR=/new/path make up
   ```

No app-level migration step is needed; QuestDB reads from the mounted host directory.

## Backfill behavior (operational notes)

- Date range mode (`--from/--to`) is UTC and day-inclusive.
  - `--from 20250727 --to 20250727` processes `00.lz4` through `23.lz4`.
- Per-hour backfill is idempotent (delete + reinsert in that hour window).
- Direct backfill command prints a detailed summary (`files missing`, failed download/decompress/dedup/ingest, parse errors).
- Most per-hour failures are skipped and counted.
- AWS auth/permission errors are fatal and stop the run with actionable guidance.

## Admin CLI (when `serve` is running)

```bash
cargo run -- admin status --config config.toml
cargo run -- admin stats --config config.toml
cargo run -- admin sync --config config.toml
cargo run -- admin backfill --from 20250727 --to 20250728 --config config.toml
cargo run -- admin purge --before 20250801 --config config.toml
cargo run -- admin reindex --config config.toml
```

Notes:
- Concurrent ingestion/admin jobs are rejected with a failed-precondition style error (for example: backfill already running).
- `status` shows aggregate counters (`files missing`, `files failed`).
- `purge` drops partitions before the given date for `fills` and `fills_quarantine`.

## Troubleshooting

### AWS requester-pays / credential errors
- Ensure AWS credentials are present for the process running backfill.
- Ensure permissions include requester-pays `GetObject` access.
- Typical fatal failures include `AccessDenied`, `ExpiredToken`, and missing credentials.

### Missing hourly files
- `S3 object missing; skipping ...` is non-fatal.
- This can happen for true gaps or hours not yet published.

### Decompression failures
- Backfill skips the bad hour and continues.
- Set `keep_temp_files = true` in `[backfill]` to keep `.lz4/.jsonl` artifacts in `temp_dir` for debugging.

### Admin command can’t connect
- Start server first: `cargo run -- serve --config config.toml`
- Verify `grpc.host` / `grpc.port` in config.

### Queries return too much data / Python memory pressure
- Use smaller time ranges and filters (`coin`, `wallet`, `side`, `crossed_only`).
- Python SDK materializes streamed rows into DataFrames.

## Development checks

```bash
cargo fmt --check
cargo clippy --all-targets --all-features
```
