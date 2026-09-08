# ChangeLake Phase 1
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

COMPOSE := docker compose
ENV_FILE := .env

.PHONY: help up down reset seed jars bootstrap wait status ps logs mysql-cli flink-sql

help:
	@echo "ChangeLake Phase 1 targets:"
	@echo "  make jars       Download Paimon + shaded Hadoop jars into flink/lib/"
	@echo "  make up         cp .env.example .env (if missing) && compose up -d"
	@echo "  make wait       Wait until MySQL + Flink UI are healthy"
	@echo "  make seed       Re-apply deterministic seed (seed=42) into MySQL"
	@echo "  make down       Stop containers (keep named volumes)"
	@echo "  make reset      down -v + remove volumes, then up + wait"
	@echo "  make status     Compose ps + Flink overview curl"
	@echo "  make mysql-cli  mysql client into changelake DB"
	@echo "  make bootstrap  jars + up + wait"

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
