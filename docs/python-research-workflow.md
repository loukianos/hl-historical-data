# Python Research Workflow

This guide is for researchers using this repo on a shared or production research machine.

## 1) One-time setup

From the repo root:

```bash
bash scripts/setup_python_research_env.sh --with-jupyter
```

This will:
- create a virtual environment (default: `.venv`)
- install the Python SDK + dev dependencies (`pytest`, etc.)
- optionally install Jupyter tooling and register a notebook kernel

### Custom options

```bash
bash scripts/setup_python_research_env.sh --venv /opt/venvs/hl-data --python python3.11
```

Useful flags:
- `--venv <path>`: choose env location
- `--python <binary>`: choose Python version
- `--with-jupyter`: install/register notebook tools

## 2) Daily workflow

Activate the environment:

```bash
source .venv/bin/activate
```

Start Jupyter (if installed):

```bash
jupyter lab
```

In notebooks, select kernel: **Python (hl-historical-data)**.

When done:

```bash
deactivate
```

## 3) After pulling new changes

Re-activate and reinstall editable package to pick up dependency/code changes:

```bash
source .venv/bin/activate
pip install -e src/clients/python/hl-historical-client[dev]
```

If proto/schema changes landed:

```bash
make proto-python
```

## 4) Validate local Python setup

```bash
source .venv/bin/activate
python -c "import hl_historical_client, polars, grpc"
pytest -q src/clients/python/hl-historical-client/tests/test_client.py
```

## 5) Recommended notebook conventions

- Keep notebooks under `notebooks/`.
- Put reusable helper code in `.py` files (for example `notebooks/lib/`) and import it from notebooks.
- Avoid committing large output-heavy notebook cells.
- Keep data access configuration outside notebooks when possible (env vars or config files).

## 6) Troubleshooting

### `No module named pytest` or `No module named polars`
You are likely outside the project venv.

```bash
source .venv/bin/activate
pip install -e src/clients/python/hl-historical-client[dev]
```

### Import errors for generated proto modules
Regenerate Python stubs and reinstall:

```bash
make proto-python
pip install -e src/clients/python/hl-historical-client[dev]
```

### Wrong notebook kernel
In Jupyter, switch to **Python (hl-historical-data)**. If missing, rerun:

```bash
bash scripts/setup_python_research_env.sh --with-jupyter
```
