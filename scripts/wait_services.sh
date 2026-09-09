#!/usr/bin/env bash
# Wait for MinIO (bucket ready), MySQL, and Flink JobManager UI.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -f .env ]]; then
  # shellcheck disable=SC1091
  set -a
  source .env
  set +a
fi

MYSQL_PORT="${MYSQL_PORT:-3306}"
FLINK_UI_PORT="${FLINK_UI_PORT:-8081}"
MINIO_API_PORT="${MINIO_API_PORT:-9000}"
TIMEOUT="${WAIT_TIMEOUT:-180}"

echo "[wait] MinIO :${MINIO_API_PORT}, MySQL :${MYSQL_PORT}, Flink UI :${FLINK_UI_PORT} (timeout=${TIMEOUT}s)"

deadline=$((SECONDS + TIMEOUT))

minio_ok=0
mysql_ok=0
flink_ok=0

while (( SECONDS < deadline )); do
  if (( minio_ok == 0 )); then
    if curl -sf "http://localhost:${MINIO_API_PORT}/minio/health/live" >/dev/null 2>&1; then
      minio_ok=1
      echo "[wait] MinIO healthy (http://localhost:${MINIO_API_PORT})"
    fi
  fi
  if (( mysql_ok == 0 )); then
    if docker compose exec -T mysql mysqladmin ping -h 127.0.0.1 -uroot -pchangelake --silent 2>/dev/null; then
      mysql_ok=1
      echo "[wait] MySQL healthy"
    fi
  fi
  if (( flink_ok == 0 )); then
    if curl -sf "http://localhost:${FLINK_UI_PORT}/overview" >/dev/null 2>&1; then
      flink_ok=1
      echo "[wait] Flink UI healthy (http://localhost:${FLINK_UI_PORT})"
    fi
  fi
  if (( minio_ok == 1 && mysql_ok == 1 && flink_ok == 1 )); then
    if docker compose ps minio-init 2>/dev/null | grep -qiE 'exited \(0\)|exit 0|Completed'; then
      echo "[wait] minio-init completed (bucket ready)"
    fi
    echo "[wait] all services ready"
    exit 0
  fi
  sleep 3
done

echo "[wait] TIMEOUT waiting for services (minio_ok=${minio_ok} mysql_ok=${mysql_ok} flink_ok=${flink_ok})" >&2
docker compose ps >&2 || true
exit 1
