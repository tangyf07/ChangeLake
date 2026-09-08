# ChangeLake Phase 2 (G1–G4 CDC) — Phase 1 targets retained
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

COMPOSE := docker compose
ENV_FILE := .env

.PHONY: help up down reset seed jars bootstrap wait status ps logs mysql-cli flink-sql \
	pipeline stop-pipeline demo mutate-insert mutate-update mutate-delete

help:
	@echo "ChangeLake Phase 2 targets:"
	@echo "  make jars            Download Paimon + Hadoop + Flink CDC + MySQL JDBC jars"
	@echo "  make up              cp .env.example .env (if missing) && compose up -d"
	@echo "  make wait            Wait until MySQL + Flink UI are healthy"
	@echo "  make seed            Re-apply deterministic seed (seed=42) into MySQL"
	@echo "  make pipeline        Start MySQL CDC → Paimon ODS job"
	@echo "  make stop-pipeline   Cancel ODS CDC job"
	@echo "  make demo            Golden Path G1–G4 (bash scripts/demo_golden_path.sh)"
	@echo "  make down / reset / status / mysql-cli / bootstrap"
	@echo "  Note: FLINK_UI_PORT from .env (default 8081; use 18081 if busy)"

jars:
	bash scripts/bootstrap.sh --jars-only

up:
	@if [[ ! -f $(ENV_FILE) ]]; then cp .env.example $(ENV_FILE); echo "created $(ENV_FILE) from .env.example"; fi
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

reset:
	$(COMPOSE) down -v --remove-orphans
	@if [[ ! -f $(ENV_FILE) ]]; then cp .env.example $(ENV_FILE); fi
	$(COMPOSE) up -d
	bash scripts/wait_services.sh

seed:
	bash scripts/seed.sh

bootstrap: jars up wait

wait:
	bash scripts/wait_services.sh

pipeline:
	bash scripts/start_pipeline.sh

stop-pipeline:
	bash scripts/stop_pipeline.sh

demo:
	bash scripts/demo_golden_path.sh

mutate-insert:
	bash scripts/mutate_insert.sh

mutate-update:
	bash scripts/mutate_update.sh

mutate-delete:
	bash scripts/mutate_delete.sh

status:
	$(COMPOSE) ps
	@curl -sf http://localhost:$${FLINK_UI_PORT:-8081}/overview | head -c 400 || true
	@echo

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs --tail=100

mysql-cli:
	$(COMPOSE) exec mysql mysql -uchangelake -pchangelake changelake

flink-sql:
	$(COMPOSE) exec jobmanager ./bin/sql-client.sh
