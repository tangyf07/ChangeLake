#!/usr/bin/env bash
# ChangeLake Phase 2 Golden Path: G1–G4 only (snapshot / insert / update / delete).
# Output format: spec §22. Hard fail → exit 2 (never WARNING-and-continue).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/scripts/common.sh"

EVIDENCE_DIR="$ROOT/docs/evidence"
mkdir -p "$EVIDENCE_DIR"

G1_STATUS=PENDING
G2_STATUS=PENDING
G3_STATUS=PENDING
G4_STATUS=PENDING

fail_case() {
  local id="$1"
  local title="$2"
  shift 2
  echo "[${id}] FAIL ${title}"
  printf '%s\n' "$@"
  case "$id" in
    G1) G1_STATUS=FAIL ;;
    G2) G2_STATUS=FAIL ;;
    G3) G3_STATUS=FAIL ;;
    G4) G4_STATUS=FAIL ;;
  esac
  print_summary
  exit 2
}

pass_case() {
  local id="$1"
  local title="$2"
  echo "[${id}] PASS ${title}"
  case "$id" in
    G1) G1_STATUS=PASS ;;
    G2) G2_STATUS=PASS ;;
    G3) G3_STATUS=PASS ;;
    G4) G4_STATUS=PASS ;;
  esac
}

print_summary() {
  cat <<SUM

==================================================
ChangeLake Golden Path (Phase 2: G1–G4)
==================================================

G1  Initial Snapshot       ${G1_STATUS}
G2  Insert                 ${G2_STATUS}
G3  Update                 ${G3_STATUS}
G4  Delete                 ${G4_STATUS}

SUM
  if [[ "$G1_STATUS" == PASS && "$G2_STATUS" == PASS && "$G3_STATUS" == PASS && "$G4_STATUS" == PASS ]]; then
    echo "ALL PASS (G1–G4)"
  else
    echo "FAILED"
  fi
}

wait_until() {
  # wait_until <timeout_s> <description> <bash condition...>
  local timeout="$1"
  local desc="$2"
  shift 2
  local deadline=$((SECONDS + timeout))
  echo "[wait] ${desc} (timeout=${timeout}s)"
  while (( SECONDS < deadline )); do
    if "$@"; then
      return 0
    fi
    sleep "$CDC_POLL_INTERVAL"
  done
  echo "[wait] TIMEOUT: ${desc}" >&2
  return 1
}

# --- Preconditions ---
echo "[demo] ChangeLake Phase 2 Golden Path G1–G4"
echo "[demo] Flink UI port: ${FLINK_UI_PORT} → $(flink_ui)"
bash "$ROOT/scripts/wait_services.sh"

# Clean MySQL mutation residue + re-seed deterministic baseline
echo "[demo] re-seed MySQL (seed=42) and ensure order 900001 absent"
bash "$ROOT/scripts/seed.sh"
mysql_exec -e "DELETE FROM orders WHERE order_id = 900001;" >/dev/null || true

SRC_USERS="$(mysql_scalar "SELECT COUNT(*) FROM changelake.users;")"
SRC_ORDERS="$(mysql_scalar "SELECT COUNT(*) FROM changelake.orders;")"
SRC_ITEMS="$(mysql_scalar "SELECT COUNT(*) FROM changelake.order_items;")"
if [[ "$SRC_USERS" != "20" || "$SRC_ORDERS" != "50" || "$SRC_ITEMS" != "85" ]]; then
  echo "[demo] unexpected MySQL seed counts: users=${SRC_USERS} orders=${SRC_ORDERS} items=${SRC_ITEMS}" >&2
  exit 2
fi

# Start (or restart) CDC pipeline → recreates ODS tables + initial snapshot
bash "$ROOT/scripts/stop_pipeline.sh" || true
bash "$ROOT/scripts/start_pipeline.sh"

# ==================================================
# G1 Initial Snapshot
# ==================================================
echo
echo "=================================================="
echo "[G1] Initial Snapshot"
echo "=================================================="

check_g1_counts() {
  local u o i
  u="$(ods_count ods_users)" || return 1
  o="$(ods_count ods_orders)" || return 1
  i="$(ods_count ods_order_items)" || return 1
  [[ "$u" == "20" && "$o" == "50" && "$i" == "85" ]]
}

if ! wait_until "$CDC_WAIT_TIMEOUT" "ODS counts == MySQL (20/50/85)" check_g1_counts; then
  u="$(ods_count ods_users 2>/dev/null || echo '?')"
  o="$(ods_count ods_orders 2>/dev/null || echo '?')"
  i="$(ods_count ods_order_items 2>/dev/null || echo '?')"
  fail_case G1 "initial snapshot" \
    "source.users=${SRC_USERS} lake.ods_users=${u}" \
    "source.orders=${SRC_ORDERS} lake.ods_orders=${o}" \
    "source.order_items=${SRC_ITEMS} lake.ods_order_items=${i}"
fi

LAKE_USERS="$(ods_count ods_users)"
LAKE_ORDERS="$(ods_count ods_orders)"
LAKE_ITEMS="$(ods_count ods_order_items)"

echo "source.users           = ${SRC_USERS}"
echo "lake.ods_users         = ${LAKE_USERS}"
echo "source.orders          = ${SRC_ORDERS}"
echo "lake.ods_orders        = ${LAKE_ORDERS}"
echo "source.order_items     = ${SRC_ITEMS}"
echo "lake.ods_order_items   = ${LAKE_ITEMS}"

# Spot-check fixed PKs
spot_users="$(paimon_sql <<'SQL'
SELECT user_id, username, city FROM ods.ods_users WHERE user_id = 1;
SQL
)"
spot_orders="$(paimon_sql <<'SQL'
SELECT order_id, user_id, status, CAST(amount AS STRING) FROM ods.ods_orders WHERE order_id = 1;
SQL
)"
spot_items="$(paimon_sql <<'SQL'
SELECT item_id, order_id, product_id, qty FROM ods.ods_order_items WHERE item_id = 1;
SQL
)"

echo "$spot_users" | grep -q 'user_001' || fail_case G1 "initial snapshot" "spot-check users PK=1 missing user_001" "$spot_users"
echo "$spot_users" | grep -q 'Hangzhou' || fail_case G1 "initial snapshot" "spot-check users PK=1 city != Hangzhou" "$spot_users"
echo "$spot_orders" | grep -Eq '110([.]0+)?|110.00' || fail_case G1 "initial snapshot" "spot-check orders PK=1 amount != 110.00" "$spot_orders"
echo "$spot_orders" | grep -q 'paid' || fail_case G1 "initial snapshot" "spot-check orders PK=1 status != paid" "$spot_orders"
echo "$spot_items" | grep -q '18' || fail_case G1 "initial snapshot" "spot-check items PK=1 product_id != 18" "$spot_items"

{
  echo "G1 Initial Snapshot"
  echo "source.users=${SRC_USERS} lake.ods_users=${LAKE_USERS}"
  echo "source.orders=${SRC_ORDERS} lake.ods_orders=${LAKE_ORDERS}"
  echo "source.order_items=${SRC_ITEMS} lake.ods_order_items=${LAKE_ITEMS}"
  echo "spot users: $spot_users"
  echo "spot orders: $spot_orders"
  echo "spot items: $spot_items"
  echo "PASS"
} >"$EVIDENCE_DIR/g1_initial_snapshot.txt"

pass_case G1 "initial snapshot"

# ==================================================
# G2 INSERT
# ==================================================
echo
echo "=================================================="
echo "[G2] Insert"
echo "=================================================="

bash "$ROOT/scripts/mutate_insert.sh"

check_g2() {
  local out
  out="$(ods_orders_query 900001)" || return 1
  echo "$out" | grep -q '900001' || return 1
  echo "$out" | grep -q 'created' || return 1
  echo "$out" | grep -Eq '99[.]50|99.5' || return 1
}

if ! wait_until "$CDC_WAIT_TIMEOUT" "ods_orders contains order_id=900001" check_g2; then
  out="$(ods_orders_query 900001 2>/dev/null || true)"
  fail_case G2 "insert" "expected order_id=900001 status=created amount=99.50" "$out"
fi

G2_OUT="$(ods_orders_query 900001)"
echo "$G2_OUT"
{
  echo "G2 Insert order_id=900001"
  echo "$G2_OUT"
  echo "PASS"
} >"$EVIDENCE_DIR/g2_insert.txt"
pass_case G2 "insert"

# ==================================================
# G3 UPDATE
# ==================================================
echo
echo "=================================================="
echo "[G3] Update"
echo "=================================================="

bash "$ROOT/scripts/mutate_update.sh"

check_g3() {
  local out cnt
  out="$(ods_orders_query 900001)" || return 1
  echo "$out" | grep -q 'paid' || return 1
  echo "$out" | grep -Eq '199[.]99' || return 1
  # Ensure single current-state row: count must be 1
  cnt="$(paimon_sql <<'SQL'
SELECT COUNT(*) FROM ods.ods_orders WHERE order_id = 900001;
SQL
)"
  cnt="$(echo "$cnt" | extract_plain_scalar)" || return 1
  [[ "$cnt" == "1" ]]
}

if ! wait_until "$CDC_WAIT_TIMEOUT" "ods_orders 900001 → amount=199.99 status=paid (single row)" check_g3; then
  out="$(ods_orders_query 900001 2>/dev/null || true)"
  fail_case G3 "update" "expected single row amount=199.99 status=paid" "$out"
fi

G3_OUT="$(ods_orders_query 900001)"
echo "$G3_OUT"
{
  echo "G3 Update order_id=900001 → amount=199.99 status=paid"
  echo "$G3_OUT"
  echo "PASS"
} >"$EVIDENCE_DIR/g3_update.txt"
pass_case G3 "update"

# ==================================================
# G4 DELETE
# ==================================================
echo
echo "=================================================="
echo "[G4] Delete"
echo "=================================================="

bash "$ROOT/scripts/mutate_delete.sh"

check_g4() {
  local cnt
  cnt="$(paimon_sql <<'SQL'
SELECT COUNT(*) FROM ods.ods_orders WHERE order_id = 900001;
SQL
)"
  cnt="$(echo "$cnt" | extract_plain_scalar)" || return 1
  [[ "$cnt" == "0" ]]
}

if ! wait_until "$CDC_WAIT_TIMEOUT" "ods_orders order_id=900001 absent" check_g4; then
  out="$(ods_orders_query 900001 2>/dev/null || true)"
  fail_case G4 "delete" "expected empty current-state for order_id=900001" "$out"
fi

{
  echo "G4 Delete order_id=900001"
  echo "current-state count=0"
  echo "PASS"
} >"$EVIDENCE_DIR/g4_delete.txt"
pass_case G4 "delete"

print_summary
exit 0
