#!/usr/bin/env bash
# Start MySQL CDC → Paimon ODS streaming pipeline (Phase 2).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/scripts/common.sh"

echo "[pipeline] ensuring CDC grants on MySQL user"
mysql_root_exec < "$ROOT/mysql/003_cdc_grants.sql"

echo "[pipeline] ensuring jars present (Paimon + paimon-s3 + CDC + MySQL JDBC)"
need_jars=0
compgen -G "$ROOT/flink/lib/flink-sql-connector-mysql-cdc-*.jar" >/dev/null || need_jars=1
compgen -G "$ROOT/flink/lib/paimon-flink-*.jar" >/dev/null || need_jars=1
compgen -G "$ROOT/flink/lib/paimon-s3-*.jar" >/dev/null || need_jars=1
compgen -G "$ROOT/flink/lib/mysql-connector-j-*.jar" >/dev/null || need_jars=1
if (( need_jars == 1 )); then
  bash "$ROOT/scripts/bootstrap.sh" --jars-only
fi

# Jars are copied into /opt/flink/lib only at container start. Restart if CDC jar missing inside.
if ! docker compose exec -T jobmanager bash -lc 'compgen -G "/opt/flink/lib/flink-sql-connector-mysql-cdc-*.jar" >/dev/null' \
  || ! docker compose exec -T jobmanager bash -lc 'compgen -G "/opt/flink/lib/paimon-s3-*.jar" >/dev/null'; then
  echo "[pipeline] CDC/paimon-s3 jar not in JM lib yet — restarting jobmanager/taskmanager to pick up /jars"
  docker compose restart jobmanager taskmanager
  bash "$ROOT/scripts/wait_services.sh"
fi

echo "[pipeline] canceling previous '${PIPELINE_JOB_NAME}' jobs (if any)"
cancel_flink_jobs "$PIPELINE_JOB_NAME" || true
# Wait until no active job with that name remains
deadline=$((SECONDS + 60))
while (( SECONDS < deadline )); do
  if python3 -c "
import json, urllib.request, sys
ui, needle = sys.argv[1], sys.argv[2]
active = {'RUNNING','RESTARTING','CREATED','INITIALIZING','SUSPENDED','CANCELLING'}
data = json.load(urllib.request.urlopen(ui + '/jobs/overview'))
for j in data.get('jobs', []):
    name = j.get('name') or ''
    state = (j.get('state') or '').upper()
    if needle in name and state in active:
        sys.exit(1)
sys.exit(0)
" "$(flink_ui)" "$PIPELINE_JOB_NAME" 2>/dev/null; then
    break
  fi
  sleep 2
done

echo "[pipeline] ensuring Paimon catalog exists"
# Flink SQL does not support CREATE CATALOG IF NOT EXISTS (ParseException on NOT).
if ! docker compose exec -T jobmanager ./bin/sql-client.sh -e "SHOW CATALOGS;" 2>/dev/null | grep -q paimon; then
  docker compose exec -T jobmanager ./bin/sql-client.sh -f /opt/flink/sql-changelake/paimon_catalog.sql
fi

echo "[pipeline] submitting Flink SQL: flink/sql/submit_ods_pipeline.sql"
# Detached: streaming STATEMENT SET blocks the sql-client session.
docker compose exec -d jobmanager ./bin/sql-client.sh -f /opt/flink/sql-changelake/submit_ods_pipeline.sql

wait_flink_job_running "$PIPELINE_JOB_NAME" "${CDC_WAIT_TIMEOUT}"
echo "[pipeline] started. Flink UI: $(flink_ui)"
echo "[pipeline] job name: ${PIPELINE_JOB_NAME}"
