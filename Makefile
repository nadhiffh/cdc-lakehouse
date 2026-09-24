.PHONY: setup data up down seed register replay verify reset build test run ui logs clean

export CDC_ROOT := $(shell pwd)
export DBT_PROFILES_DIR := $(shell pwd)/dbt

VENV := .venv/bin
CONNECTOR := docker/connector-postgres.json

setup:
	uv venv --python 3.12
	uv pip install -r requirements.txt

data:
	$(VENV)/python -m pipeline.download

# Waits for health checks, so the next target can assume a working stack.
up:
	docker compose up -d --wait

down:
	docker compose down

seed:
	$(VENV)/python -m pipeline.seed

# PUT to /config rather than POST to /connectors, so re-registering an existing
# connector updates it instead of failing with 409.
register:
	@curl -sf -X PUT -H "Content-Type: application/json" \
		--data "$$(python3 -c 'import json,sys; print(json.dumps(json.load(open("$(CONNECTOR)"))["config"]))')" \
		http://localhost:8083/connectors/commerce-connector/config > /dev/null \
		&& echo "connector registered" || { echo "connector registration failed"; exit 1; }

replay:
	$(VENV)/python -m pipeline.replay

consume:
	$(VENV)/python -m pipeline.consume

verify:
	$(VENV)/python -m pipeline.verify_stream

# TRUNCATE writes no per-row deletes to the WAL, so a reseed without dropping
# topics and the replication slot silently stacks duplicate generations in
# Kafka. `reset` is the only safe way back to a clean state.
reset:
	$(VENV)/python -m pipeline.reset

deps:
	cd dbt && $(CURDIR)/$(VENV)/dbt deps

build: deps
	cd dbt && $(CURDIR)/$(VENV)/dbt build

test: deps
	cd dbt && $(CURDIR)/$(VENV)/dbt test

# Full pipeline from a genuinely clean state. This is the path that surfaced
# both real bugs in this project, so it is the one worth running before a push.
# `wait-snapshot` blocks until Debezium finishes the initial snapshot, otherwise
# replay would race it and the reconciliation would compare partial state.
rebuild: reset seed register wait-snapshot replay wait-stream consume build
	@echo "clean rebuild complete"

wait-snapshot:
	$(VENV)/python -m pipeline.wait --for snapshot

wait-stream:
	$(VENV)/python -m pipeline.wait --for stream

logs:
	docker compose logs -f connect

ui:
	docker compose --profile ui up -d kafka-ui
	@echo "Kafka UI on http://localhost:8080"

clean:
	rm -rf data/warehouse.duckdb dbt/target
