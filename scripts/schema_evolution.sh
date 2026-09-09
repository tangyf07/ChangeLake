#!/usr/bin/env bash
# Phase 3 / G5: explicit schema evolution for ADD COLUMN channel.
#
# Honest support model (Flink 1.18.1 + mysql-cdc 3.1.1 SQL connector + Paimon 1.4.2):
# - Flink SQL mysql-cdc table schema is FIXED at job submit → no transparent runtime DDL.
# - Transparent schema evolution exists on Flink CDC *Pipeline YAML* API, not this SQL path.
# - Supported demo path: ALTER MySQL → ALTER Paimon ADD COLUMN → resubmit evolved Flink SQL
#   (no ODS DROP / full lake rebuild). See docs/schema-evolution.md.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/scripts/common.sh"

APPLY_DML=0
SKIP_RESUBMIT=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --with-dml) APPLY_DML=1; shift ;;
    --ddl-only) SKIP_RESUBMIT=1; shift ;;
    -h|--help)
      echo "Usage: $0 [--ddl-only] [--with-dml]"
      echo "  default: MySQL ALTER + Paimon ADD + resubmit evolved CDC job"
      echo "  --ddl-only   stop after MySQL ALTER (assert pipeline still RUNNING)"
      echo "  --with-dml   also apply schema_evolution_dml.sql after evolved job is RUNNING"
      exit 0
      ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

echo "[schema_evolution] Phase 3 explicit migration: ADD COLUMN orders.channel"

if ! flink_job_is_running "$PIPELINE_JOB_NAME"; then
  echo "[schema_evolution] FAIL: expected pipeline '${PIPELINE_JOB_NAME}' RUNNING before MySQL ALTER" >&2
  exit 2
fi
echo "[schema_evolution] pre-ALTER: pipeline RUNNING"

echo "[schema_evolution] applying mysql/mutations/schema_evolution.sql (while pipeline RUNNING)"
docker compose exec -T mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" < "$ROOT/mysql/mutations/schema_evolution.sql"

ch_cnt="$(mysql_scalar "
SELECT COUNT(*) FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA='changelake' AND TABLE_NAME='orders' AND COLUMN_NAME='channel';
")"
if [[ "$ch_cnt" != "1" ]]; then
  echo "[schema_evolution] FAIL: MySQL orders.channel missing after ALTER" >&2
  exit 2
fi
echo "[schema_evolution] MySQL orders.channel present"

# Give the still-running (pre-evolution schema) job a moment; ADD COLUMN is usually tolerated
# by Debezium/mysql-cdc as ignored extra fields. We assert RUNNING, not silent DDL sync.
sleep 3
if ! flink_job_is_running "$PIPELINE_JOB_NAME"; then
  echo "[schema_evolution] FAIL: pipeline left RUNNING state after MySQL ALTER" >&2
  exit 2
fi
echo "[schema_evolution] post-MySQL-ALTER: pipeline still RUNNING (pre-evolution Flink schema)"

if (( SKIP_RESUBMIT == 1 )); then
  echo "[schema_evolution] --ddl-only: skipping Paimon ALTER / job resubmit"
  exit 0
fi

echo "[schema_evolution] ensuring Paimon ods.ods_orders.channel via ALTER TABLE ADD"
if paimon_column_exists ods ods_orders channel; then
  echo "[schema_evolution] Paimon channel already present"
else
  paimon_sql <<'SQL'
ALTER TABLE ods.ods_orders ADD channel STRING;
SQL
  echo "[schema_evolution] Paimon ALTER TABLE ods.ods_orders ADD channel STRING — done"
fi

if ! paimon_column_exists ods ods_orders channel; then
  echo "[schema_evolution] FAIL: Paimon ods_orders.channel still missing" >&2
  exit 2
fi

echo "[schema_evolution] canceling pre-evolution job and submitting evolved SQL"
cancel_flink_jobs "$PIPELINE_JOB_NAME" || true
deadline=$((SECONDS + 60))
while (( SECONDS < deadline )); do
  if ! flink_job_is_running "$PIPELINE_JOB_NAME"; then
    break
  fi
  sleep 2
done

docker compose exec -d jobmanager ./bin/sql-client.sh -f /opt/flink/sql-changelake/submit_ods_pipeline_evolved.sql
wait_flink_job_running "$PIPELINE_JOB_NAME" "${CDC_WAIT_TIMEOUT}"
echo "[schema_evolution] evolved pipeline RUNNING (mysql_orders + ods_orders include channel)"

if (( APPLY_DML == 1 )); then
  echo "[schema_evolution] applying mysql/mutations/schema_evolution_dml.sql"
  docker compose exec -T mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" < "$ROOT/mysql/mutations/schema_evolution_dml.sql"
fi

echo "[schema_evolution] done"
