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

register:
	curl -sf -X POST -H "Content-Type: application/json" \
		--data @$(CONNECTOR) http://localhost:8083/connectors \
		| python3 -m json.tool

replay:
	$(VENV)/python -m pipeline.replay

verify:
	$(VENV)/python -m pipeline.verify_stream

# TRUNCATE writes no per-row deletes to the WAL, so a reseed without dropping
# topics and the replication slot silently stacks duplicate generations in
# Kafka. `reset` is the only safe way back to a clean state.
reset:
	$(VENV)/python -m pipeline.reset

build:
	cd dbt && $(CURDIR)/$(VENV)/dbt build

test:
	cd dbt && $(CURDIR)/$(VENV)/dbt test

logs:
	docker compose logs -f connect

ui:
	docker compose --profile ui up -d kafka-ui
	@echo "Kafka UI on http://localhost:8080"

clean:
	rm -rf data/warehouse.duckdb dbt/target
