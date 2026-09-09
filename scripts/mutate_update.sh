#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/scripts/common.sh"
echo "[mutate] UPDATE order_id=900001 amount=199.99 status=paid"
docker compose exec -T mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" < "$ROOT/mysql/mutations/update.sql"
mysql_scalar "SELECT order_id, status, amount FROM changelake.orders WHERE order_id=900001;"
