.PHONY: up down logs reset build run serve backfill-sync proto-python

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
	python3 -m grpc_tools.protoc \
		-I../../../../proto \
		--python_out=src/hl_historical_client/proto \
		--pyi_out=src/hl_historical_client/proto \
		--grpc_python_out=src/hl_historical_client/proto \
		../../../../proto/hl_historical.proto

sdk-install:
	pip install -e src/clients/python/hl-historical-client
