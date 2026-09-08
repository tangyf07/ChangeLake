#!/usr/bin/env bash
# Wait for MySQL and Flink JobManager UI (8081).
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
TIMEOUT="${WAIT_TIMEOUT:-180}"

echo "[wait] MySQL :${MYSQL_PORT} and Flink UI :${FLINK_UI_PORT} (timeout=${TIMEOUT}s)"

deadline=$((SECONDS + TIMEOUT))

mysql_ok=0
flink_ok=0

while (( SECONDS < deadline )); do
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
  if (( mysql_ok == 1 && flink_ok == 1 )); then
    echo "[wait] all services ready"
    exit 0
  fi
  sleep 3
done

echo "[wait] TIMEOUT waiting for services (mysql_ok=${mysql_ok} flink_ok=${flink_ok})" >&2
docker compose ps >&2 || true
exit 1
