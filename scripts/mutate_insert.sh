#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/scripts/common.sh"
echo "[mutate] INSERT order_id=900001"
docker compose exec -T mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" < "$ROOT/mysql/mutations/insert.sql"
mysql_scalar "SELECT order_id, status, amount FROM changelake.orders WHERE order_id=900001;"
