#!/usr/bin/env bash
# Re-apply deterministic seed SQL (seed=42) into running MySQL.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "[seed] applying mysql/002_seed.sql (seed=42)"
docker compose exec -T mysql mysql -uchangelake -pchangelake changelake < mysql/002_seed.sql
echo "[seed] verifying counts"
docker compose exec -T mysql mysql -uchangelake -pchangelake -N -e "
SELECT 'users', COUNT(*) FROM changelake.users
UNION ALL SELECT 'orders', COUNT(*) FROM changelake.orders
UNION ALL SELECT 'order_items', COUNT(*) FROM changelake.order_items;
"
echo "[seed] expected: users=20 orders=50 order_items=85"
