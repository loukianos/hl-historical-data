# hl-historical-client

Polars-native Python SDK for the Hyperliquid historical data service.

## Install (editable)

> Recommended: use a modern pip (PEP 660 support), e.g. `pip>=22`.

From the repo root:

```bash
pip install -e src/clients/python/hl-historical-client
```

Or from this directory:

```bash
pip install -e .
```

During build/editable install, protobuf + gRPC Python stubs are generated automatically from the included `proto/hl_historical.proto` into `src/hl_historical_client/proto/`.

## Regenerate stubs manually

From repo root:

```bash
make proto-python
```

If your proto file is in a non-standard location, set `HL_HISTORICAL_PROTO_PATH=/absolute/path/to/hl_historical.proto`.

## Local server connection

This SDK is designed for a local trusted setup. By default, `HistoricalDataClient()` connects to `127.0.0.1:50051` (no TLS/auth).

From the repository root, a typical flow is:

```bash
make up
cp config.example.toml config.toml
cargo run -- serve --config config.toml
```

In another terminal, ingest data (required before examples return meaningful results):

```bash
cargo run -- backfill --sync --config config.toml
```

> Note: backfill pulls from Hyperliquid S3 archives and needs valid AWS credentials / requester-pays access.

For full startup/troubleshooting guidance, see the root `README.md` and `docs/python-research-workflow.md`.

## Usage

```python
from datetime import datetime, timedelta, timezone

from hl_historical_client import HistoricalDataClient

end = datetime.now(timezone.utc)
start = end - timedelta(hours=1)

with HistoricalDataClient() as client:  # defaults to 127.0.0.1:50051
    fills = client.get_fills(start=start, end=end, coin="BTC")

    vwap = client.get_vwap(
        coin="BTC",
        interval="1m",
        start=start,
        end=end,
    )

    summary = client.get_wallet_summary(
        wallet="0x...",
        start=start,
        end=end,
    )
    # summary["summary"] and summary["per_coin"] are Polars DataFrames
```

All query methods return Polars DataFrames. Naive datetimes are treated as UTC.

Supported intervals: `1s`, `5s`, `30s`, `1m`, `5m`, `15m`, `30m`, `1h`, `4h`, `1d`.

## Examples

Example scripts live in `examples/`:

- `basic_fills_query.py`
- `vwap_and_bars.py`
- `wallet_summary.py`

Run from repo root:

```bash
python src/clients/python/hl-historical-client/examples/basic_fills_query.py --hours 1 --coin BTC
python src/clients/python/hl-historical-client/examples/vwap_and_bars.py --coin BTC --vwap-interval 1m --bars-interval 5m --hours 6
python src/clients/python/hl-historical-client/examples/wallet_summary.py --wallet 0x... --hours 24
```

Or from this package directory:

```bash
python examples/basic_fills_query.py --hours 1 --coin BTC
python examples/vwap_and_bars.py --coin BTC --hours 6
python examples/wallet_summary.py --wallet 0x... --hours 24
```

Examples assume you have already run an editable install (which generates gRPC stubs) and that the local gRPC server is running with ingested data. Start with small time windows and use `--coin` / `--wallet` filters to avoid loading very large result sets into memory.

## Research machine setup

From the repository root, you can bootstrap a full Python research environment (venv + SDK + optional Jupyter kernel):

```bash
bash scripts/setup_python_research_env.sh --with-jupyter
```

For workflow guidance, see `docs/python-research-workflow.md`.
