#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/scripts/common.sh"
echo "[mutate] DELETE order_id=900001"
docker compose exec -T mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" < "$ROOT/mysql/mutations/delete.sql"
cnt="$(mysql_scalar "SELECT COUNT(*) FROM changelake.orders WHERE order_id=900001;")"
echo "[mutate] mysql remaining rows for 900001: ${cnt}"
