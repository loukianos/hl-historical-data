.PHONY: up down logs reset build build-release run serve backfill-sync check proto-python sdk-install

PYTHON ?= python3

# ─── Docker Compose ──────────────────────────────────────────────────

up:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

reset:
	@echo "This will delete all QuestDB and Redis data. Press Ctrl+C to cancel."
	@sleep 3
	docker compose down -v
	rm -rf .data/

# ─── Rust ────────────────────────────────────────────────────────────

build:
	cargo build

build-release:
	cargo build --release

serve:
	cargo run --release -- serve --config config.example.toml

backfill-sync:
	cargo run --release -- backfill --sync --config config.example.toml

check:
	cargo fmt --check
	cargo clippy -- -D warnings

# ─── Python SDK ──────────────────────────────────────────────────────

proto-python:
	cd src/clients/python/hl-historical-client && \
	$(PYTHON) setup.py generate_proto

sdk-install:
	pip install -e src/clients/python/hl-historical-client
