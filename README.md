# hl-historical-data

Standalone Rust service for ingesting Hyperliquid historical fills into QuestDB and querying them over gRPC.

## Quickstart

1. Start local dependencies:

```bash
make up
```

2. Create a local config from the example:

```bash
cp config.example.toml config.toml
```

3. Build the project:

```bash
cargo build
```

4. Run the gRPC service:

```bash
cargo run -- serve --config config.toml
```

5. Run a backfill (date range):

```bash
cargo run -- backfill --from 20250727 --to 20250727 --config config.toml
```

## Development checks

```bash
cargo fmt --check
cargo clippy --all-targets --all-features
```

Current `clippy` output includes placeholder `dead_code` warnings from scaffold modules and generated protobuf code. These are expected at Milestone 0 and should disappear as implementation issues are completed.
