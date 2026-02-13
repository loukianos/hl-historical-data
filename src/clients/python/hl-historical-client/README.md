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

## Usage

```python
from datetime import datetime

from hl_historical_client import HistoricalDataClient

with HistoricalDataClient(host="127.0.0.1", port=50051) as client:
    fills = client.get_fills(
        start=datetime(2025, 1, 1, 0, 0, 0),
        end=datetime(2025, 1, 2, 0, 0, 0),
        coin="BTC",
    )

    vwap = client.get_vwap(
        coin="BTC",
        interval="1m",
        start=datetime(2025, 1, 1, 0, 0, 0),
        end=datetime(2025, 1, 2, 0, 0, 0),
    )

    summary = client.get_wallet_summary(
        wallet="0x...",
        start=datetime(2025, 1, 1, 0, 0, 0),
        end=datetime(2025, 1, 2, 0, 0, 0),
    )
    # summary["summary"] and summary["per_coin"] are Polars DataFrames
```

All query methods return Polars DataFrames. Naive datetimes are treated as UTC.

Supported intervals: `1s`, `5s`, `30s`, `1m`, `5m`, `15m`, `30m`, `1h`, `4h`, `1d`.
