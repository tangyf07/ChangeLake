#!/usr/bin/env bash
# Start Phase 5 DWD (+ optional ADS batch refresh) Flink SQL jobs.
# Order: cancel stuck jobs → submit DWD → wait ≥1 DWD checkpoint (committed rows)
#        → then submit ADS batch INSERT OVERWRITE (must FINISH, not stay SCHEDULED).
# Prerequisites: MinIO/MySQL/Flink up; ODS pipeline preferably RUNNING with channel column.
# Cluster needs taskmanager.numberOfTaskSlots ≥10 (see docker-compose FLINK_PROPERTIES).
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
echo "[dwd-ads] skip SHOW CATALOGS (sql-client -e can hang; submit SQL creates catalog in-session)"

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

echo "[dwd-ads] canceling previous '${DWD_JOB_NAME}' / '${ADS_JOB_NAME}' jobs (free slots for DWD + collect)"
cancel_flink_jobs "$ADS_JOB_NAME" || true
cancel_flink_jobs "$DWD_JOB_NAME" || true
deadline=$((SECONDS + 60))
while (( SECONDS < deadline )); do
  if ! flink_job_is_running "$DWD_JOB_NAME" 2>/dev/null \
    && ! flink_job_is_running "$ADS_JOB_NAME" 2>/dev/null; then
    break
  fi
  sleep 2
done

echo "[dwd-ads] submitting DWD streaming job: flink/sql/submit_dwd_pipeline.sql"
docker compose exec -d jobmanager ./bin/sql-client.sh -f /opt/flink/sql-changelake/submit_dwd_pipeline.sql
wait_flink_job_running "$DWD_JOB_NAME" "${CDC_WAIT_TIMEOUT}"

# Paimon streaming sink commits on checkpoint — wait before ADS / verify polls
echo "[dwd-ads] waiting for DWD committed data (≥1 completed checkpoint)"
wait_flink_checkpoint 1 "${CDC_WAIT_TIMEOUT}" "$DWD_JOB_NAME"

# Belt-and-suspenders: confirm ≥1 DWD row is queryable (cancel stray ADS so collect can schedule)
cancel_flink_jobs "$ADS_JOB_NAME" || true
wait_dwd_rows_visible() {
  local cnt
  cnt="$(paimon_sql <<'SQL' | extract_plain_scalar
SELECT COUNT(*) FROM dwd.dwd_orders;
SQL
)" || return 1
  echo "[dwd-ads] dwd.dwd_orders count=${cnt}"
  [[ -n "$cnt" && "$cnt" != "0" ]] || return 1
  python3 -c "import sys; sys.exit(0 if int(sys.argv[1]) >= 1 else 1)" "$cnt"
}

deadline=$((SECONDS + CDC_WAIT_TIMEOUT))
echo "[dwd-ads] polling DWD row visibility (timeout=${CDC_WAIT_TIMEOUT}s)"
while (( SECONDS < deadline )); do
  if wait_dwd_rows_visible; then
    break
  fi
  sleep "$CDC_POLL_INTERVAL"
done
if ! wait_dwd_rows_visible; then
  echo "[dwd-ads] ERROR: dwd.dwd_orders still empty after checkpoint gate" >&2
  exit 1
fi

wait_ads_job_finished() {
  # Prefer FINISHED; treat missing job after a successful sql-client submit as OK if already done.
  local timeout="${1:-$CDC_WAIT_TIMEOUT}"
  local deadline=$((SECONDS + timeout))
  echo "[dwd-ads] waiting for ADS job '*${ADS_JOB_NAME}*' FINISHED (timeout=${timeout}s)"
  while (( SECONDS < deadline )); do
    if python3 -c "
import json, urllib.request, sys
ui, needle = sys.argv[1], sys.argv[2]
data = json.load(urllib.request.urlopen(ui + '/jobs/overview', timeout=10))
jobs = [j for j in data.get('jobs', []) if needle in (j.get('name') or '')]
if not jobs:
    sys.exit(2)
# newest first by default in overview; accept any FINISHED
states = [(j.get('state') or '').upper() for j in jobs]
print('ADS states=' + ','.join(states))
if any(s == 'FINISHED' for s in states):
    sys.exit(0)
# still active / waiting for slots
active = {'RUNNING','RESTARTING','CREATED','INITIALIZING','RECONCILING','CANCELLING'}
if any(s in active for s in states):
    sys.exit(1)
# FAILED / CANCELED
sys.exit(3)
" "$(flink_ui)" "$ADS_JOB_NAME"; then
      echo "[dwd-ads] ADS job FINISHED"
      return 0
    fi
    rc=$?
    if (( rc == 3 )); then
      echo "[dwd-ads] ERROR: ADS job ended in FAILED/CANCELED" >&2
      return 1
    fi
    sleep 3
  done
  echo "[dwd-ads] TIMEOUT waiting for ADS FINISHED — canceling stuck ADS" >&2
  cancel_flink_jobs "$ADS_JOB_NAME" || true
  return 1
}

if [[ "$SKIP_ADS" != "1" ]]; then
  echo "[dwd-ads] submitting ADS batch refresh: flink/sql/submit_ads_pipeline.sql"
  cancel_flink_jobs "$ADS_JOB_NAME" || true
  # Batch INSERT OVERWRITE must run to FINISHED (execution.runtime-mode=batch in SQL)
  if ! docker compose exec -T jobmanager ./bin/sql-client.sh -f /opt/flink/sql-changelake/submit_ads_pipeline.sql; then
    echo "[dwd-ads] WARN: ADS submit returned non-zero (check sql-client output)" >&2
  fi
  wait_ads_job_finished "${CDC_WAIT_TIMEOUT}" || {
    echo "[dwd-ads] ERROR: ADS batch did not reach FINISHED (slot starvation or query error?)" >&2
    echo "[dwd-ads] hint: ensure taskmanager.numberOfTaskSlots≥10 then recreate TM: docker compose up -d --force-recreate taskmanager" >&2
    exit 1
  }
  echo "[dwd-ads] ADS batch refresh finished (job name: ${ADS_JOB_NAME})"
else
  echo "[dwd-ads] SKIP_ADS=1 — DWD only"
fi

echo "[dwd-ads] started. Flink UI: $(flink_ui)"
echo "[dwd-ads] DWD job: ${DWD_JOB_NAME}"
