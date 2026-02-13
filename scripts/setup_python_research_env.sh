#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SDK_DIR="$ROOT_DIR/src/clients/python/hl-historical-client"
VENV_DIR="$ROOT_DIR/.venv"
PYTHON_BIN="python3"
WITH_JUPYTER=0
KERNEL_NAME="hl-historical-data"
KERNEL_DISPLAY_NAME="Python (hl-historical-data)"

usage() {
  cat <<'EOF'
Usage: scripts/setup_python_research_env.sh [options]

Options:
  --venv <path>           Virtual environment directory (default: .venv at repo root)
  --python <binary>       Python binary to use (default: python3)
  --with-jupyter          Install jupyterlab + ipykernel and register a kernel
  --kernel-name <name>    Kernel name when using --with-jupyter (default: hl-historical-data)
  --kernel-display <name> Kernel display name when using --with-jupyter
  -h, --help              Show help

Examples:
  scripts/setup_python_research_env.sh
  scripts/setup_python_research_env.sh --with-jupyter
  scripts/setup_python_research_env.sh --venv /opt/venvs/hl-data --python python3.11
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --venv)
      VENV_DIR="$2"
      shift 2
      ;;
    --python)
      PYTHON_BIN="$2"
      shift 2
      ;;
    --with-jupyter)
      WITH_JUPYTER=1
      shift
      ;;
    --kernel-name)
      KERNEL_NAME="$2"
      shift 2
      ;;
    --kernel-display)
      KERNEL_DISPLAY_NAME="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ ! -d "$SDK_DIR" ]]; then
  echo "Could not find SDK directory at: $SDK_DIR" >&2
  exit 1
fi

echo "[1/4] Creating virtual environment at $VENV_DIR"
"$PYTHON_BIN" -m venv "$VENV_DIR"

# shellcheck disable=SC1091
source "$VENV_DIR/bin/activate"

echo "[2/4] Upgrading pip tooling"
python -m pip install --upgrade pip setuptools wheel

echo "[3/4] Installing SDK with dev dependencies"
pip install -e "$SDK_DIR[dev]"

if [[ "$WITH_JUPYTER" -eq 1 ]]; then
  echo "[4/4] Installing Jupyter tooling and registering kernel"
  pip install jupyterlab ipykernel
  python -m ipykernel install --user --name "$KERNEL_NAME" --display-name "$KERNEL_DISPLAY_NAME"
else
  echo "[4/4] Skipping Jupyter tooling (use --with-jupyter to enable)"
fi

echo
cat <<EOF
Done.

Activate this environment with:
  source "$VENV_DIR/bin/activate"

Run SDK tests with:
  pytest -q "$SDK_DIR/tests/test_client.py"

Deactivate when finished:
  deactivate
EOF
