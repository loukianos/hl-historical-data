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
