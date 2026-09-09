#!/usr/bin/env bash
# ChangeLake Phase 6 / G7 verification:
#   inject deliberate DWD amount corruption for BACKFILL_DT → backfill twice →
#   fingerprint(run1)==fingerprint(run2) → reconcile already asserted inside backfill.
# Prints [G7] PASS backfill on success. Exit 2 on hard fail.
# Does NOT require re-running full CDC; MySQL remains source of truth.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/scripts/common.sh"

EVIDENCE_DIR="$ROOT/docs/evidence"
EVIDENCE_FILE="$EVIDENCE_DIR/g7_backfill.txt"
mkdir -p "$EVIDENCE_DIR"

# Prefer 2026-08-13 (seed has orders 12 + 39); fall back to Phase 5 CHECK_DT
BACKFILL_DT="${BACKFILL_DT:-2026-08-13}"
ADS_JOB_NAME="${ADS_JOB_NAME:-changelake-ads-order-daily}"

fail() {
  echo "[G7] FAIL $*"
  {
    echo "G7 Backfill / Phase 6"
    echo "BACKFILL_DT=${BACKFILL_DT}"
    echo "FAIL: $*"
    echo "NOT claimed: EO-2PC / general backfill orchestrator / G8–G10 / full CDC replay"
  } >"$EVIDENCE_FILE"
  exit 2
}

echo "=================================================="
echo "[G7] Backfill / Phase 6"
echo "=================================================="
echo "[g7] BACKFILL_DT=${BACKFILL_DT}"

# Ensure DWD has rows (P5 should have run; if empty, try start_dwd_ads)
dwd_total="$(paimon_sql <<'SQL' | extract_plain_scalar
SELECT COUNT(*) FROM dwd.dwd_orders;
SQL
)" || dwd_total=0
if [[ -z "$dwd_total" || "$dwd_total" == "0" ]]; then
  echo "[g7] dwd.dwd_orders empty — submitting DWD/ADS via start_dwd_ads.sh"
  bash "$ROOT/scripts/start_dwd_ads.sh" || fail "start_dwd_ads.sh failed"
fi

# Confirm MySQL has rows for BACKFILL_DT
mysql_n="$(mysql_scalar "SELECT COUNT(*) FROM orders WHERE DATE(order_ts)='${BACKFILL_DT}';")" \
  || fail "MySQL count for BACKFILL_DT failed"
if [[ "$mysql_n" == "0" ]]; then
  fail "MySQL has 0 orders for BACKFILL_DT=${BACKFILL_DT} (pick a seed day)"
fi
echo "[g7] MySQL orders on ${BACKFILL_DT}: ${mysql_n}"

# ---- Inject deliberate DWD corruption (amount+1 / net_amount+1) for that dt ----
# MySQL stays correct → backfill repairs FROM MySQL (bypass CDC).
echo "[g7] corrupting DWD amounts (+1) for dt=${BACKFILL_DT}"
cancel_flink_jobs "$ADS_JOB_NAME" || true
CORRUPT_SQL="${TMPDIR:-/tmp}/changelake_g7_corrupt.sql"
cat >"$CORRUPT_SQL" <<SQL
SET 'pipeline.name' = 'changelake-g7-corrupt-${BACKFILL_DT}';
SET 'execution.runtime-mode' = 'batch';
SET 'parallelism.default' = '1';

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = 's3://changelake/warehouse',
  's3.endpoint' = 'http://minio:9000',
  's3.access-key' = 'minioadmin',
  's3.secret-key' = 'minioadmin',
  's3.path.style.access' = 'true'
);

USE CATALOG paimon;

-- Upsert corrupted amounts for the logical partition (PK merge)
INSERT INTO dwd.dwd_orders
SELECT
  order_id,
  user_id,
  status,
  CAST(amount + 1 AS DECIMAL(12, 2)) AS amount,
  channel,
  coupon_amount,
  CAST(net_amount + 1 AS DECIMAL(12, 2)) AS net_amount,
  order_ts,
  updated_at
FROM dwd.dwd_orders
WHERE CAST(order_ts AS DATE) = DATE '${BACKFILL_DT}';
SQL

docker compose cp "$CORRUPT_SQL" jobmanager:/tmp/changelake_g7_corrupt.sql >/dev/null
if ! docker compose exec -T jobmanager ./bin/sql-client.sh -f /tmp/changelake_g7_corrupt.sql \
    >"${TMPDIR:-/tmp}/changelake_g7_corrupt.out" 2>&1; then
  cat "${TMPDIR:-/tmp}/changelake_g7_corrupt.out" >&2 || true
  fail "DWD corruption submit failed"
fi
if grep -q '\[ERROR\]' "${TMPDIR:-/tmp}/changelake_g7_corrupt.out"; then
  cat "${TMPDIR:-/tmp}/changelake_g7_corrupt.out" >&2 || true
  fail "DWD corruption SQL error"
fi

# Prove corruption: DWD sum should be MySQL sum + N
mysql_amt="$(mysql_scalar "SELECT ROUND(SUM(amount),2) FROM orders WHERE DATE(order_ts)='${BACKFILL_DT}';")"
dwd_bad="$(paimon_sql <<SQL | extract_plain_scalar
SELECT CAST(SUM(net_amount) AS STRING) FROM dwd.dwd_orders WHERE CAST(order_ts AS DATE) = DATE '${BACKFILL_DT}';
SQL
)" || fail "post-corrupt DWD sum failed"
python3 -c "
import sys
m,d,n=float(sys.argv[1]),float(sys.argv[2]),int(sys.argv[3])
sys.exit(0 if abs(d-(m+n))<0.001 else 1)
" "$mysql_amt" "$dwd_bad" "$mysql_n" || {
  echo "[g7] WARN: expected DWD sum ≈ MySQL+${mysql_n} (got mysql=${mysql_amt} dwd=${dwd_bad}); continuing if dwd!=mysql"
  python3 -c "import sys; sys.exit(0 if abs(float(sys.argv[1])-float(sys.argv[2]))>0.001 else 1)" \
    "$mysql_amt" "$dwd_bad" || fail "corruption did not change DWD vs MySQL"
}
echo "[g7] corruption visible: MySQL sum=${mysql_amt} DWD sum=${dwd_bad}"

# ---- Backfill run 1 ----
echo "[g7] backfill run #1"
out1="$(bash "$ROOT/scripts/backfill.sh" "$BACKFILL_DT")" || fail "backfill run1 failed"
echo "$out1"
fp1="$(echo "$out1" | awk -F= '/^BACKFILL_FINGERPRINT=/{print $2}' | tail -n1)"
[[ -n "$fp1" ]] || fail "missing BACKFILL_FINGERPRINT from run1"

# ---- Backfill run 2 (idempotency) ----
echo "[g7] backfill run #2 (idempotency)"
out2="$(bash "$ROOT/scripts/backfill.sh" "$BACKFILL_DT")" || fail "backfill run2 failed"
echo "$out2"
fp2="$(echo "$out2" | awk -F= '/^BACKFILL_FINGERPRINT=/{print $2}' | tail -n1)"
[[ -n "$fp2" ]] || fail "missing BACKFILL_FINGERPRINT from run2"

if [[ "$fp1" != "$fp2" ]]; then
  fail "fingerprint mismatch run1=${fp1} run2=${fp2}"
fi
echo "[g7] fingerprints equal: ${fp1}"

{
  echo "G7 Backfill / Phase 6"
  echo "BACKFILL_DT=${BACKFILL_DT}"
  echo "corruption=DWD amount+1 and net_amount+1 for CAST(order_ts AS DATE)=dt (MySQL untouched)"
  echo "repair=MySQL snapshot → DELETE+INSERT DWD for dt → DELETE+INSERT ADS for dt"
  echo "fingerprint_def=SHA256(canonical_sorted_DWD_lines) + SHA256(canonical_sorted_ADS_lines) combined"
  echo "fingerprint_run1=${fp1}"
  echo "fingerprint_run2=${fp2}"
  echo "idempotent=yes (run1==run2)"
  echo "paid_status_set=('paid','shipped','completed')"
  echo "null_channel_ads_literal=unknown"
  echo "NOT claimed: EO-2PC / Exactly-Once E2E / general orchestrator / G8–G10 / time travel / compaction"
  echo "PASS"
} >"$EVIDENCE_FILE"

echo
echo "[G7] PASS backfill"
echo "[g7] evidence: ${EVIDENCE_FILE}"
exit 0
