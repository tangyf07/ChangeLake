#!/usr/bin/env bash
# Shared helpers for ChangeLake scripts. Source me; do not execute.

: "${ROOT:?ROOT must be set before sourcing common.sh}"
cd "$ROOT"

if [[ -f "$ROOT/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/.env"
  set +a
fi

MYSQL_USER="${MYSQL_USER:-changelake}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-changelake}"
MYSQL_DATABASE="${MYSQL_DATABASE:-changelake}"
FLINK_UI_PORT="${FLINK_UI_PORT:-8081}"
PIPELINE_JOB_NAME="${PIPELINE_JOB_NAME:-changelake-ods-cdc}"
CDC_WAIT_TIMEOUT="${CDC_WAIT_TIMEOUT:-180}"
CDC_POLL_INTERVAL="${CDC_POLL_INTERVAL:-5}"

# MinIO / Paimon S3 warehouse (demo-only defaults; match .env.example)
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_BUCKET="${MINIO_BUCKET:-changelake}"
PAIMON_WAREHOUSE="${PAIMON_WAREHOUSE:-s3://changelake/warehouse}"
S3_PATH_STYLE_ACCESS="${S3_PATH_STYLE_ACCESS:-true}"

mysql_exec() {
  docker compose exec -T mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" "$MYSQL_DATABASE" "$@"
}

mysql_root_exec() {
  docker compose exec -T mysql mysql -uroot -p"${MYSQL_ROOT_PASSWORD:-changelake}" "$@"
}

mysql_scalar() {
  docker compose exec -T mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" -N -e "$1" | tr -d '\r' | head -n1 | awk '{print $1}'
}

flink_ui() {
  echo "http://localhost:${FLINK_UI_PORT}"
}

cancel_flink_jobs() {
  local needle="${1:-$PIPELINE_JOB_NAME}"
  python3 -c "
import json, urllib.request, sys
needle = sys.argv[1]
ui = sys.argv[2]
try:
    data = json.load(urllib.request.urlopen(ui + '/jobs/overview'))
except Exception as e:
    print(f'[common] jobs overview failed: {e}', file=sys.stderr)
    sys.exit(0)
active = {'RUNNING','RESTARTING','CREATED','INITIALIZING','SUSPENDED','CANCELLING'}
for j in data.get('jobs', []):
    name = j.get('name') or ''
    jid = j.get('jid') or j.get('id')
    state = (j.get('state') or '').upper()
    if needle in name and state in active and jid:
        print(f'[common] canceling {jid} name={name} state={state}', file=sys.stderr)
        req = urllib.request.Request(ui + f'/jobs/{jid}?mode=cancel', method='PATCH')
        try:
            urllib.request.urlopen(req)
        except Exception as e:
            print(f'[common] cancel failed: {e}', file=sys.stderr)
" "$needle" "$(flink_ui)"
}

wait_flink_job_running() {
  local needle="${1:-$PIPELINE_JOB_NAME}"
  local timeout="${2:-$CDC_WAIT_TIMEOUT}"
  local deadline=$((SECONDS + timeout))
  echo "[common] waiting for Flink job '*${needle}*' RUNNING (timeout=${timeout}s)"
  while (( SECONDS < deadline )); do
    if python3 -c "
import json, urllib.request, sys
ui, needle = sys.argv[1], sys.argv[2]
data = json.load(urllib.request.urlopen(ui + '/jobs/overview'))
for j in data.get('jobs', []):
    name = j.get('name') or ''
    state = (j.get('state') or '').upper()
    if needle in name and state == 'RUNNING':
        print(j.get('jid') or j.get('id'))
        sys.exit(0)
sys.exit(1)
" "$(flink_ui)" "$needle" 2>/dev/null; then
      echo "[common] job RUNNING"
      return 0
    fi
    sleep 3
  done
  echo "[common] TIMEOUT waiting for job RUNNING" >&2
  curl -sf "$(flink_ui)/jobs/overview" >&2 || true
  return 1
}

paimon_catalog_ddl() {
  # Official Paimon 1.4.2 S3/MinIO options:
  # https://paimon.apache.org/docs/1.4/maintenance/filesystems/
  cat <<SQL
CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = '${PAIMON_WAREHOUSE}',
  's3.endpoint' = '${MINIO_ENDPOINT}',
  's3.access-key' = '${MINIO_ROOT_USER}',
  's3.secret-key' = '${MINIO_ROOT_PASSWORD}',
  's3.path.style.access' = '${S3_PATH_STYLE_ACCESS}'
);
SQL
}

paimon_sql() {
  local tmp sql_file
  tmp="$(mktemp)"
  sql_file="$(mktemp)"
  {
    cat <<HDR
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';
-- Flink has no CREATE CATALOG IF NOT EXISTS; catalog is session-scoped.
-- Warehouse: MinIO S3 (not local file:///).
$(paimon_catalog_ddl)
USE CATALOG paimon;
HDR
    cat
  } >"$sql_file"
  docker compose cp "$sql_file" jobmanager:/tmp/changelake_query.sql >/dev/null
  if ! docker compose exec -T jobmanager ./bin/sql-client.sh -f /tmp/changelake_query.sql >"$tmp" 2>&1; then
    local rc=$?
    echo "[common] paimon_sql failed (rc=$rc)" >&2
    cat "$tmp" >&2
    rm -f "$tmp" "$sql_file"
    return "$rc"
  fi
  if grep -q '\[ERROR\]' "$tmp"; then
    echo "[common] paimon_sql statement error:" >&2
    cat "$tmp" >&2
    rm -f "$tmp" "$sql_file"
    return 1
  fi
  cat "$tmp"
  rm -f "$tmp" "$sql_file"
}

extract_plain_scalar() {
  # Tableau rows look like: | 20 |  or | 199.99 |
  python3 -c "import re,sys
text=sys.stdin.read(); vals=[]
for line in text.splitlines():
    if '|' not in line: continue
    s=line.strip()
    if set(s)<=set('+-| '): continue
    low=s.lower()
    if 'row' in low and 'set' in low: continue
    for p in [x.strip() for x in line.split('|') if x.strip()!='']:
        if re.fullmatch(r'-?\\d+(\\.\\d+)?', p): vals.append(p)
print(vals[-1]) if vals else sys.exit(1)"
}


ods_count() {
  local table="$1"
  local out
  out="$(paimon_sql <<SQL
SELECT COUNT(*) FROM ods.${table};
SQL
)"
  echo "$out" | extract_plain_scalar
}

ods_orders_query() {
  local oid="$1"
  paimon_sql <<SQL
SELECT order_id, user_id, status, CAST(amount AS STRING) AS amount
FROM ods.ods_orders
WHERE order_id = ${oid};
SQL
}
