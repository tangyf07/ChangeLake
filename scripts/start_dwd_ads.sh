#!/usr/bin/env bash
# Start Phase 5 DWD (+ optional ADS batch refresh) Flink SQL jobs.
# Prerequisites: MinIO/MySQL/Flink up; ODS pipeline preferably RUNNING with channel column.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/scripts/common.sh"

DWD_JOB_NAME="${DWD_JOB_NAME:-changelake-dwd-orders}"
ADS_JOB_NAME="${ADS_JOB_NAME:-changelake-ads-order-daily}"
SKIP_ADS="${SKIP_ADS:-0}"

echo "[dwd-ads] ensuring jars present"
need_jars=0
compgen -G "$ROOT/flink/lib/paimon-flink-*.jar" >/dev/null || need_jars=1
compgen -G "$ROOT/flink/lib/paimon-s3-*.jar" >/dev/null || need_jars=1
if (( need_jars == 1 )); then
  bash "$ROOT/scripts/bootstrap.sh" --jars-only
fi

echo "[dwd-ads] ensuring Paimon catalog exists"
if ! docker compose exec -T jobmanager ./bin/sql-client.sh -e "SHOW CATALOGS;" 2>/dev/null | grep -q paimon; then
  docker compose exec -T jobmanager ./bin/sql-client.sh -f /opt/flink/sql-changelake/paimon_catalog.sql
fi

# Ensure dwd/ads DDL (fresh sql-client session + CREATE CATALOG inside dwd.sql/ads.sql)
echo "[dwd-ads] ensuring dwd/ads DDL"
docker compose exec -T jobmanager ./bin/sql-client.sh -f /opt/flink/sql-changelake/dwd.sql >/tmp/changelake_dwd_ddl.out 2>&1 || true
docker compose exec -T jobmanager ./bin/sql-client.sh -f /opt/flink/sql-changelake/ads.sql >/tmp/changelake_ads_ddl.out 2>&1 || true

# ODS must expose channel for DWD SELECT
if ! paimon_column_exists ods ods_orders channel; then
  echo "[dwd-ads] ERROR: ods.ods_orders.channel missing — run schema evolution (G5) or evolved ODS first" >&2
  echo "[dwd-ads] hint: bash scripts/start_pipeline.sh && bash scripts/schema_evolution.sh" >&2
  exit 1
fi

echo "[dwd-ads] canceling previous '${DWD_JOB_NAME}' jobs (if any)"
cancel_flink_jobs "$DWD_JOB_NAME" || true
deadline=$((SECONDS + 60))
while (( SECONDS < deadline )); do
  if ! flink_job_is_running "$DWD_JOB_NAME" 2>/dev/null; then
    break
  fi
  sleep 2
done

echo "[dwd-ads] submitting DWD streaming job: flink/sql/submit_dwd_pipeline.sql"
docker compose exec -d jobmanager ./bin/sql-client.sh -f /opt/flink/sql-changelake/submit_dwd_pipeline.sql
wait_flink_job_running "$DWD_JOB_NAME" "${CDC_WAIT_TIMEOUT}"

if [[ "$SKIP_ADS" != "1" ]]; then
  echo "[dwd-ads] waiting briefly for DWD rows before ADS batch refresh"
  sleep 8
  echo "[dwd-ads] submitting ADS batch refresh: flink/sql/submit_ads_pipeline.sql"
  # Batch INSERT OVERWRITE is short-lived; run attached and ignore "job finished" as success
  if ! docker compose exec -T jobmanager ./bin/sql-client.sh -f /opt/flink/sql-changelake/submit_ads_pipeline.sql; then
    echo "[dwd-ads] WARN: ADS submit returned non-zero (check sql-client output)" >&2
  fi
  echo "[dwd-ads] ADS batch refresh submitted (job name: ${ADS_JOB_NAME})"
else
  echo "[dwd-ads] SKIP_ADS=1 — DWD only"
fi

echo "[dwd-ads] started. Flink UI: $(flink_ui)"
echo "[dwd-ads] DWD job: ${DWD_JOB_NAME}"
