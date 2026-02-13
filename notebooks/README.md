# Notebooks

This folder contains research notebooks for `hl-historical-data`.

## Environment setup (one-time)

From repo root:

```bash
bash scripts/setup_python_research_env.sh --with-jupyter
```

Then activate before notebook work:

```bash
source .venv/bin/activate
```

Use kernel: **Python (hl-historical-data)**.

## Folder conventions

- `notebooks/` — notebooks (`*.ipynb`)
- `notebooks/lib/` — reusable Python helpers imported by notebooks
- `notebooks/data/` — small local scratch artifacts only (do not commit large datasets)

## Recommended workflow

1. Keep heavy reusable logic in `.py` modules under `notebooks/lib/`.
2. Keep notebooks focused on analysis and visualization.
3. Clear large outputs before committing.
4. Re-sync SDK deps after pulls:

```bash
source .venv/bin/activate
pip install -e src/clients/python/hl-historical-client[dev]
```

## Quick sanity checks

```bash
source .venv/bin/activate
python -c "import hl_historical_client, polars"
pytest -q src/clients/python/hl-historical-client/tests/test_client.py
```

## Related docs

- `docs/python-research-workflow.md`
- `src/clients/python/hl-historical-client/README.md`
