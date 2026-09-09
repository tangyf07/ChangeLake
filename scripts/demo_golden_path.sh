#!/usr/bin/env bash
# ChangeLake Phase 7 Golden Path: G1–G6 + P5 DWD/ADS + G7 Backfill + G8 Time Travel (not G9–G10).
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
G5_STATUS=PENDING
G6_STATUS=PENDING
P5_STATUS=PENDING
G7_STATUS=PENDING
G8_STATUS=PENDING

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
    G5) G5_STATUS=FAIL ;;
    G6) G6_STATUS=FAIL ;;
    P5|DWD/ADS) P5_STATUS=FAIL ;;
    G7) G7_STATUS=FAIL ;;
    G8) G8_STATUS=FAIL ;;
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
    G5) G5_STATUS=PASS ;;
    G6) G6_STATUS=PASS ;;
    P5|DWD/ADS) P5_STATUS=PASS ;;
    G7) G7_STATUS=PASS ;;
    G8) G8_STATUS=PASS ;;
  esac
}

print_summary() {
  cat <<SUM

==================================================
ChangeLake Golden Path (Phase 7: G1–G6 + P5 + G7 + G8)
==================================================

G1  Initial Snapshot       ${G1_STATUS}
G2  Insert                 ${G2_STATUS}
G3  Update                 ${G3_STATUS}
G4  Delete                 ${G4_STATUS}
G5  Schema Evolution       ${G5_STATUS}
G6  Failure Recovery       ${G6_STATUS}
P5  DWD + ADS              ${P5_STATUS}
G7  Backfill               ${G7_STATUS}
G8  Time Travel            ${G8_STATUS}

SUM
  if [[ "$G1_STATUS" == PASS && "$G2_STATUS" == PASS && "$G3_STATUS" == PASS && "$G4_STATUS" == PASS && "$G5_STATUS" == PASS && "$G6_STATUS" == PASS && "$P5_STATUS" == PASS && "$G7_STATUS" == PASS && "$G8_STATUS" == PASS ]]; then
    echo "ALL PASS (G1–G6 + P5 + G7 + G8)"
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
echo "[demo] ChangeLake Phase 7 Golden Path G1–G6 + P5 + G7 + G8"
echo "[demo] Flink UI port: ${FLINK_UI_PORT} → $(flink_ui)"
bash "$ROOT/scripts/wait_services.sh"

# Restore baseline MySQL schema (no channel) so G1–G4 stay Phase-2-shaped on reruns
mysql_drop_orders_channel_if_exists

# Clean MySQL mutation residue + re-seed deterministic baseline
echo "[demo] re-seed MySQL (seed=42) and ensure order 900001/900002 absent"
bash "$ROOT/scripts/seed.sh"
mysql_exec -e "DELETE FROM orders WHERE order_id IN (900001, 900002, 900003);" >/dev/null || true

SRC_USERS="$(mysql_scalar "SELECT COUNT(*) FROM changelake.users;")"
SRC_ORDERS="$(mysql_scalar "SELECT COUNT(*) FROM changelake.orders;")"
SRC_ITEMS="$(mysql_scalar "SELECT COUNT(*) FROM changelake.order_items;")"
if [[ "$SRC_USERS" != "20" || "$SRC_ORDERS" != "50" || "$SRC_ITEMS" != "85" ]]; then
  echo "[demo] unexpected MySQL seed counts: users=${SRC_USERS} orders=${SRC_ORDERS} items=${SRC_ITEMS}" >&2
  exit 2
fi

# Start (or restart) CDC pipeline → recreates ODS tables + initial snapshot (no channel)
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

# ==================================================
# G5 Schema Evolution
# ==================================================
echo
echo "=================================================="
echo "[G5] Schema Evolution"
echo "=================================================="

if ! flink_job_is_running "$PIPELINE_JOB_NAME"; then
  fail_case G5 "schema evolution" "pipeline not RUNNING before evolution"
fi

# Explicit migration: MySQL ALTER (while RUNNING) → Paimon ADD → resubmit evolved job
bash "$ROOT/scripts/schema_evolution.sh"

if ! flink_job_is_running "$PIPELINE_JOB_NAME"; then
  fail_case G5 "schema evolution" "evolved pipeline not RUNNING after resubmit"
fi

# Post-evolution DML
docker compose exec -T mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" < "$ROOT/mysql/mutations/schema_evolution_dml.sql"

check_g5_channel_app() {
  local out
  out="$(ods_orders_query_channel 1)" || return 1
  echo "$out" | grep -qE '(^|[|[:space:]])1([|[:space:]]|$)' || return 1
  echo "$out" | grep -qw 'app' || return 1
}

check_g5_channel_web() {
  local out
  out="$(ods_orders_query_channel 900002)" || return 1
  echo "$out" | grep -q '900002' || return 1
  echo "$out" | grep -qw 'web' || return 1
  echo "$out" | grep -Eq '55([.]0+)?|55.00' || return 1
}

check_g5_null_channel() {
  # Pre-evolution row that was NOT updated should allow NULL channel
  local out
  out="$(paimon_sql <<'SQL'
SELECT order_id,
       CASE WHEN channel IS NULL THEN 'IS_NULL' ELSE CAST(channel AS STRING) END AS channel_chk
FROM ods.ods_orders
WHERE order_id = 2;
SQL
)" || return 1
  echo "$out" | grep -q 'IS_NULL' || return 1
}

if ! wait_until "$CDC_WAIT_TIMEOUT" "ods_orders order_id=1 channel=app" check_g5_channel_app; then
  out="$(ods_orders_query_channel 1 2>/dev/null || true)"
  fail_case G5 "schema evolution" "expected order_id=1 channel=app" "$out"
fi

if ! wait_until "$CDC_WAIT_TIMEOUT" "ods_orders order_id=900002 channel=web" check_g5_channel_web; then
  out="$(ods_orders_query_channel 900002 2>/dev/null || true)"
  fail_case G5 "schema evolution" "expected order_id=900002 channel=web amount=55.00" "$out"
fi

if ! wait_until "$CDC_WAIT_TIMEOUT" "ods_orders order_id=2 channel NULL (pre-evolution)" check_g5_null_channel; then
  out="$(ods_orders_query_channel 2 2>/dev/null || true)"
  fail_case G5 "schema evolution" "expected order_id=2 channel NULL" "$out"
fi

if ! flink_job_is_running "$PIPELINE_JOB_NAME"; then
  fail_case G5 "schema evolution" "pipeline not RUNNING after G5 DML sync"
fi

G5_1="$(ods_orders_query_channel 1)"
G5_2="$(ods_orders_query_channel 2)"
G5_NEW="$(ods_orders_query_channel 900002)"
echo "order_id=1: $G5_1"
echo "order_id=2 (pre-evolution NULL channel): $G5_2"
echo "order_id=900002: $G5_NEW"

{
  echo "G5 Schema Evolution ADD COLUMN channel"
  echo "mode=explicit_migration (Flink SQL mysql-cdc fixed schema; see docs/schema-evolution.md)"
  echo "MySQL ALTER while pipeline RUNNING → Paimon ALTER ADD → resubmit evolved SQL"
  echo "order_id=1 channel=app:"
  echo "$G5_1"
  echo "order_id=2 channel=NULL:"
  echo "$G5_2"
  echo "order_id=900002 channel=web:"
  echo "$G5_NEW"
  echo "pipeline=RUNNING"
  echo "PASS"
} >"$EVIDENCE_DIR/g5_schema_evolution.txt"

pass_case G5 "schema evolution"

# ==================================================
# G6 Failure Recovery
# ==================================================
echo
echo "=================================================="
echo "[G6] Failure Recovery"
echo "=================================================="

if ! flink_job_is_running "$PIPELINE_JOB_NAME"; then
  fail_case G6 "failure recovery" "pipeline not RUNNING before G6"
fi

if ! bash "$ROOT/scripts/failure_recovery.sh"; then
  fail_case G6 "failure recovery" "see scripts/failure_recovery.sh / docs/evidence/g6_failure_recovery.txt"
fi

pass_case G6 "failure recovery"

# ==================================================
# P5 DWD + ADS (Phase 5 — not G7–G10)
# ==================================================
echo
echo "=================================================="
echo "[DWD/ADS] Phase 5 business metrics"
echo "=================================================="

if ! bash "$ROOT/scripts/verify_dwd_ads.sh"; then
  fail_case P5 "DWD/ADS" "see scripts/verify_dwd_ads.sh / docs/evidence/dwd_ads.txt"
fi

pass_case P5 "DWD/ADS"

# ==================================================
# G7 Backfill (Phase 6)
# ==================================================
echo
echo "=================================================="
echo "[G7] Backfill"
echo "=================================================="

if ! bash "$ROOT/scripts/verify_backfill.sh"; then
  fail_case G7 "backfill" "see scripts/verify_backfill.sh / docs/evidence/g7_backfill.txt"
fi

pass_case G7 "backfill"

# ==================================================
# G8 Time Travel (Phase 7 — not G9–G10)
# ==================================================
echo
echo "=================================================="
echo "[G8] Time Travel"
echo "=================================================="

if ! bash "$ROOT/scripts/time_travel.sh"; then
  fail_case G8 "time travel" "see scripts/time_travel.sh / docs/evidence/g8_time_travel.txt"
fi

pass_case G8 "time travel"

print_summary
exit 0
