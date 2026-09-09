# ChangeLake Phase 10 (Engineering) — G1–G10 + pytest / light CI / docs / evidence
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

COMPOSE := docker compose
ENV_FILE := .env

.PHONY: help up down reset seed jars bootstrap wait status ps logs mysql-cli flink-sql \
	pipeline stop-pipeline demo mutate-insert mutate-update mutate-delete schema-evolution \
	failure-recovery minio-init smoke-storage start-dwd-ads dwd-ads backfill time-travel reconcile compaction \
	test lint ci

help:
	@echo "ChangeLake Phase 10 targets:"
	@echo "  make jars            Download Paimon + paimon-s3 + Hadoop + Flink CDC + MySQL JDBC"
	@echo "  make up              cp .env.example .env (if missing) && compose up -d"
	@echo "  make wait            Wait until MinIO + MySQL + Flink UI are healthy"
	@echo "  make minio-init      Ensure MinIO bucket exists (idempotent)"
	@echo "  make smoke-storage   Flink → Paimon → MinIO write/read smoke (before G1)"
	@echo "  make seed            Re-apply deterministic seed (seed=42) into MySQL"
	@echo "  make pipeline        Start MySQL CDC → Paimon ODS job (baseline schema)"
	@echo "  make stop-pipeline   Cancel ODS CDC job"
	@echo "  make schema-evolution  G5 explicit migration (ADD channel + evolved resubmit)"
	@echo "  make failure-recovery  G6 TM kill + checkpoint restore (pipeline must be RUNNING)"
	@echo "  make start-dwd-ads   Submit DWD streaming + ADS batch refresh"
	@echo "  make dwd-ads         Verify DWD + ADS metrics (scripts/verify_dwd_ads.sh)"
	@echo "  make demo            Golden Path G1–G6 + P5 + G7 + G8 + G9 + G10 compaction"
	@echo "  make backfill DT=... Date-scoped DWD/ADS repair (G7); requires DT=YYYY-MM-DD"
	@echo "  make time-travel     G8 Paimon snapshot time travel (ods.ods_tt_demo)"
	@echo "  make reconcile       G9 MySQL ↔ ODS reconcile report (DECIMAL, tol=0.01)"
	@echo "  make compaction      G10 Paimon compaction demo (ods.ods_compact_demo)"
	@echo "  make test            pytest (unit + static config)"
	@echo "  make lint            python compileall + bash -n scripts"
	@echo "  make ci              lint + test (what GitHub Actions runs)"
	@echo "  make down / reset / status / mysql-cli / bootstrap"
	@echo "  Note: FLINK_UI_PORT from .env (default 8081; use 18081 if busy)"
	@echo "  Order: jars → up → wait → smoke-storage → demo  (or dwd-ads / backfill / time-travel / reconcile / compaction)"
	@echo "  CI is unit/static only; Full Golden Path is local Docker E2E (make demo)"

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

minio-init:
	bash scripts/minio_init.sh

smoke-storage:
	bash scripts/smoke_storage.sh

pipeline:
	bash scripts/start_pipeline.sh

stop-pipeline:
	bash scripts/stop_pipeline.sh

demo:
	bash scripts/demo_golden_path.sh

schema-evolution:
	bash scripts/schema_evolution.sh

failure-recovery:
	bash scripts/failure_recovery.sh

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
	@curl -sf http://localhost:$${MINIO_API_PORT:-9000}/minio/health/live && echo " MinIO live" || true

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs --tail=100

mysql-cli:
	$(COMPOSE) exec mysql mysql -uchangelake -pchangelake changelake

flink-sql:
	$(COMPOSE) exec jobmanager ./bin/sql-client.sh

start-dwd-ads:
	bash scripts/start_dwd_ads.sh

dwd-ads:
	bash scripts/verify_dwd_ads.sh

backfill:
	@if [[ -z "$(DT)" ]]; then echo "Usage: make backfill DT=YYYY-MM-DD" >&2; exit 2; fi
	bash scripts/backfill.sh "$(DT)"

time-travel:
	bash scripts/time_travel.sh

reconcile:
	bash scripts/reconcile.sh

compaction:
	bash scripts/compaction.sh

lint:
	python3 -m compileall -q python tests
	@for f in scripts/*.sh; do bash -n "$$f"; done

test:
	python3 -m pytest -q

ci: lint test
