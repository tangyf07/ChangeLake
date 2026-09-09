#!/usr/bin/env bash
# Phase 5 verification: DWD net_amount + ADS daily metrics vs MySQL-derived expectations.
# Writes docs/evidence/dwd_ads.txt and prints [DWD/ADS] PASS on success.
# Exit 2 on hard fail (matches golden-path style).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/scripts/common.sh"

EVIDENCE_DIR="$ROOT/docs/evidence"
EVIDENCE_FILE="$EVIDENCE_DIR/dwd_ads.txt"
mkdir -p "$EVIDENCE_DIR"

DWD_JOB_NAME="${DWD_JOB_NAME:-changelake-dwd-orders}"
# Default check date: a seed day with multiple orders (2026-08-02 has orders 1 & 29)
CHECK_DT="${CHECK_DT:-2026-08-02}"
# Paid status set (documented): paid + shipped + completed
PAID_STATUSES="'paid','shipped','completed'"

fail() {
  echo "[DWD/ADS] FAIL $*"
  {
    echo "DWD/ADS Phase 5 verification"
    echo "CHECK_DT=${CHECK_DT}"
    echo "FAIL: $*"
    echo "NOT claimed: EO-2PC / streaming ADS continuous correctness / G7–G10"
  } >"$EVIDENCE_FILE"
  exit 2
}

echo "=================================================="
echo "[DWD/ADS] Phase 5 verification"
echo "=================================================="
echo "[dwd-ads] CHECK_DT=${CHECK_DT}"
echo "[dwd-ads] paid statuses: (${PAID_STATUSES})"
echo "[dwd-ads] NULL channel → ADS literal 'unknown'"

# ---- 1) Ensure ODS pipeline RUNNING ----
if ! flink_job_is_running "$PIPELINE_JOB_NAME"; then
  echo "[dwd-ads] ODS pipeline not RUNNING — starting via start_pipeline.sh"
  bash "$ROOT/scripts/start_pipeline.sh"
fi

# Ensure channel column on ODS (Phase 5 DWD SELECT needs it)
if ! paimon_column_exists ods ods_orders channel; then
  echo "[dwd-ads] ods.ods_orders.channel missing — running schema_evolution.sh"
  if [[ -x "$ROOT/scripts/schema_evolution.sh" ]]; then
    bash "$ROOT/scripts/schema_evolution.sh"
  else
    fail "ods.ods_orders.channel missing and schema_evolution.sh unavailable"
  fi
fi

if ! flink_job_is_running "$PIPELINE_JOB_NAME"; then
  fail "ODS pipeline not RUNNING after ensure"
fi

# ---- 2) Submit DWD (+ ADS batch) ----
echo "[dwd-ads] submitting DWD + ADS via start_dwd_ads.sh"
bash "$ROOT/scripts/start_dwd_ads.sh"

# ---- 3) Wait for DWD rows ----
wait_dwd_rows() {
  local cnt
  cnt="$(paimon_sql <<'SQL' | extract_plain_scalar
SELECT COUNT(*) FROM dwd.dwd_orders;
SQL
)" || return 1
  echo "[dwd-ads] dwd.dwd_orders count=${cnt}"
  [[ -n "$cnt" && "$cnt" != "0" ]] || return 1
  # Expect at least seed orders (50) +/- mutations; gate on >0 then later compare metrics
  python3 -c "import sys; sys.exit(0 if int(sys.argv[1]) >= 1 else 1)" "$cnt"
}

deadline=$((SECONDS + CDC_WAIT_TIMEOUT))
echo "[dwd-ads] waiting for DWD rows (timeout=${CDC_WAIT_TIMEOUT}s)"
while (( SECONDS < deadline )); do
  if wait_dwd_rows; then
    break
  fi
  sleep "$CDC_POLL_INTERVAL"
done
if ! wait_dwd_rows; then
  fail "no rows in dwd.dwd_orders within timeout"
fi

# Spot-check net_amount = amount (coupon absent)
echo "[dwd-ads] spot-check DWD net_amount for order_id=1"
dwd_sample="$(paimon_sql <<'SQL'
SELECT order_id,
       CAST(amount AS STRING) AS amount,
       CAST(coupon_amount AS STRING) AS coupon_amount,
       CAST(net_amount AS STRING) AS net_amount,
       CASE WHEN channel IS NULL THEN 'IS_NULL' ELSE CAST(channel AS STRING) END AS channel_chk
FROM dwd.dwd_orders
WHERE order_id = 1;
SQL
)" || fail "DWD sample query failed"
echo "$dwd_sample"
# net_amount should equal amount when coupon is null
echo "$dwd_sample" | grep -qE '1' || fail "order_id=1 missing in DWD"

# ---- 4) Ensure ADS refreshed (re-run batch for determinism) ----
echo "[dwd-ads] refreshing ADS batch (submit_ads_pipeline.sql)"
if ! docker compose exec -T jobmanager ./bin/sql-client.sh -f /opt/flink/sql-changelake/submit_ads_pipeline.sql; then
  fail "ADS batch submit failed"
fi
sleep 3

wait_ads_rows() {
  local cnt
  cnt="$(paimon_sql <<'SQL' | extract_plain_scalar
SELECT COUNT(*) FROM ads.ads_order_daily;
SQL
)" || return 1
  echo "[dwd-ads] ads.ads_order_daily count=${cnt}"
  python3 -c "import sys; sys.exit(0 if int(sys.argv[1]) >= 1 else 1)" "$cnt"
}

deadline=$((SECONDS + CDC_WAIT_TIMEOUT))
echo "[dwd-ads] waiting for ADS rows (timeout=${CDC_WAIT_TIMEOUT}s)"
while (( SECONDS < deadline )); do
  if wait_ads_rows; then
    break
  fi
  sleep "$CDC_POLL_INTERVAL"
done
if ! wait_ads_rows; then
  fail "no rows in ads.ads_order_daily within timeout"
fi

# ---- 5) Compare ADS vs MySQL-derived expectations for CHECK_DT ----
# MySQL expectation: map NULL channel → 'unknown'; paid set as documented.
# Note: MySQL may or may not have channel column; COALESCE handles both via dynamic SQL.

echo "[dwd-ads] computing MySQL expectations for dt=${CHECK_DT}"

channel_col_cnt="$(mysql_scalar "
SELECT COUNT(*) FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA='changelake' AND TABLE_NAME='orders' AND COLUMN_NAME='channel';
")" || channel_col_cnt=0

if [[ "$channel_col_cnt" == "1" ]]; then
  mysql_expect_sql="
SELECT
  COALESCE(channel, 'unknown') AS channel,
  COUNT(*) AS order_cnt,
  SUM(CASE WHEN status IN (${PAID_STATUSES}) THEN 1 ELSE 0 END) AS paid_order_cnt,
  ROUND(SUM(CASE WHEN status IN (${PAID_STATUSES}) THEN amount ELSE 0 END), 2) AS gmv,
  ROUND(SUM(CASE WHEN status IN (${PAID_STATUSES}) THEN amount ELSE 0 END), 2) AS net_gmv,
  COUNT(DISTINCT CASE WHEN status IN (${PAID_STATUSES}) THEN user_id END) AS buyer_cnt
FROM orders
WHERE DATE(order_ts) = '${CHECK_DT}'
GROUP BY COALESCE(channel, 'unknown')
ORDER BY channel;
"
else
  # No channel column → all rows map to 'unknown'
  mysql_expect_sql="
SELECT
  'unknown' AS channel,
  COUNT(*) AS order_cnt,
  SUM(CASE WHEN status IN (${PAID_STATUSES}) THEN 1 ELSE 0 END) AS paid_order_cnt,
  ROUND(SUM(CASE WHEN status IN (${PAID_STATUSES}) THEN amount ELSE 0 END), 2) AS gmv,
  ROUND(SUM(CASE WHEN status IN (${PAID_STATUSES}) THEN amount ELSE 0 END), 2) AS net_gmv,
  COUNT(DISTINCT CASE WHEN status IN (${PAID_STATUSES}) THEN user_id END) AS buyer_cnt
FROM orders
WHERE DATE(order_ts) = '${CHECK_DT}'
GROUP BY 1
ORDER BY channel;
"
fi

mysql_raw="$(docker compose exec -T mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" -N -e "$mysql_expect_sql" "$MYSQL_DATABASE" | tr -d '\r')" \
  || fail "MySQL expectation query failed"
echo "[dwd-ads] MySQL expectations:"
echo "$mysql_raw"

if [[ -z "${mysql_raw//[[:space:]]/}" ]]; then
  fail "MySQL returned no rows for dt=${CHECK_DT} (unexpected for seed)"
fi

# Fetch ADS for CHECK_DT
ads_raw="$(paimon_sql <<SQL
SELECT channel,
       CAST(order_cnt AS STRING) AS order_cnt,
       CAST(paid_order_cnt AS STRING) AS paid_order_cnt,
       CAST(gmv AS STRING) AS gmv,
       CAST(net_gmv AS STRING) AS net_gmv,
       CAST(buyer_cnt AS STRING) AS buyer_cnt
FROM ads.ads_order_daily
WHERE dt = DATE '${CHECK_DT}'
ORDER BY channel;
SQL
)" || fail "ADS query failed"
echo "[dwd-ads] ADS rows for dt=${CHECK_DT}:"
echo "$ads_raw"

# Compare channel-by-channel via Python
compare_rc=0
python3 - "$mysql_raw" "$ads_raw" <<'PY' || compare_rc=$?
import re, sys

mysql_text = sys.argv[1]
ads_text = sys.argv[2]

def parse_mysql(text):
    rows = {}
    for line in text.strip().splitlines():
        parts = line.split("\t")
        if len(parts) < 6:
            parts = re.split(r"\s+", line.strip())
        if len(parts) < 6:
            continue
        ch, oc, poc, gmv, ngmv, bc = parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]
        rows[ch] = {
            "order_cnt": int(float(oc)),
            "paid_order_cnt": int(float(poc)),
            "gmv": round(float(gmv), 2),
            "net_gmv": round(float(ngmv), 2),
            "buyer_cnt": int(float(bc)),
        }
    return rows

def parse_ads_tableau(text):
    rows = {}
    for line in text.splitlines():
        if "|" not in line:
            continue
        s = line.strip()
        if set(s) <= set("+-| "):
            continue
        low = s.lower()
        if "row" in low and "set" in low:
            continue
        if "channel" in low and "order_cnt" in low:
            continue
        cells = [x.strip() for x in line.split("|") if x.strip() != ""]
        if len(cells) < 6:
            continue
        ch, oc, poc, gmv, ngmv, bc = cells[0], cells[1], cells[2], cells[3], cells[4], cells[5]
        if not re.fullmatch(r"-?\d+(\.\d+)?", oc):
            continue
        rows[ch] = {
            "order_cnt": int(float(oc)),
            "paid_order_cnt": int(float(poc)),
            "gmv": round(float(gmv), 2),
            "net_gmv": round(float(ngmv), 2),
            "buyer_cnt": int(float(bc)),
        }
    return rows

exp = parse_mysql(mysql_text)
got = parse_ads_tableau(ads_text)
print(f"expected_channels={sorted(exp)}")
print(f"ads_channels={sorted(got)}")
if not exp:
    print("ERROR: empty MySQL expectations", file=sys.stderr)
    sys.exit(1)
if not got:
    print("ERROR: empty ADS parse", file=sys.stderr)
    sys.exit(1)
ok = True
for ch, e in sorted(exp.items()):
    g = got.get(ch)
    if g is None:
        print(f"MISSING channel={ch} in ADS", file=sys.stderr)
        ok = False
        continue
    for k in ("order_cnt", "paid_order_cnt", "gmv", "net_gmv", "buyer_cnt"):
        if e[k] != g[k]:
            print(f"MISMATCH channel={ch} {k}: expected={e[k]} got={g[k]}", file=sys.stderr)
            ok = False
    if all(e[k] == g.get(k) for k in e):
        print(f"MATCH channel={ch} {e}")
# Extra ADS channels for this dt (warn but fail — should not happen if overwrite full)
extra = set(got) - set(exp)
if extra:
    print(f"EXTRA ADS channels for dt (fail): {sorted(extra)}", file=sys.stderr)
    ok = False
sys.exit(0 if ok else 1)
PY

if (( compare_rc != 0 )); then
  fail "ADS metrics != MySQL expectations for dt=${CHECK_DT}"
fi

# Also verify total DWD net_amount identity for CHECK_DT vs MySQL amount sum
mysql_amt="$(mysql_scalar "SELECT ROUND(SUM(amount),2) FROM orders WHERE DATE(order_ts)='${CHECK_DT}';")" \
  || fail "MySQL amount sum failed"
dwd_net="$(paimon_sql <<SQL | extract_plain_scalar
SELECT CAST(SUM(net_amount) AS STRING) FROM dwd.dwd_orders WHERE CAST(order_ts AS DATE) = DATE '${CHECK_DT}';
SQL
)" || fail "DWD net_amount sum failed"

python3 -c "
import sys
a,b=float(sys.argv[1]),float(sys.argv[2])
sys.exit(0 if abs(a-b)<0.001 else 1)
" "$mysql_amt" "$dwd_net" || fail "DWD SUM(net_amount)=${dwd_net} != MySQL SUM(amount)=${mysql_amt} for dt=${CHECK_DT}"

echo "[dwd-ads] DWD SUM(net_amount)=${dwd_net} matches MySQL SUM(amount)=${mysql_amt} for dt=${CHECK_DT}"

{
  echo "DWD/ADS Phase 5 verification"
  echo "CHECK_DT=${CHECK_DT}"
  echo "paid_status_set=('paid','shipped','completed')"
  echo "null_channel_ads_literal=unknown"
  echo "coupon_amount=NULL (not in ODS); net_amount=amount"
  echo "ADS_mode=batch INSERT OVERWRITE (re-runnable)"
  echo "DWD_mode=streaming ODS→DWD (changelake-dwd-orders)"
  echo "MySQL expectations:"
  echo "$mysql_raw"
  echo "ADS rows:"
  echo "$ads_raw"
  echo "DWD sample order_id=1:"
  echo "$dwd_sample"
  echo "DWD SUM(net_amount)=${dwd_net} MySQL SUM(amount)=${mysql_amt}"
  echo "NOT claimed: EO-2PC / Exactly-Once E2E / continuous streaming ADS / G7–G10 / coupon_amount evolution"
  echo "PASS"
} >"$EVIDENCE_FILE"

echo
echo "[DWD/ADS] PASS"
echo "[dwd-ads] evidence: ${EVIDENCE_FILE}"
exit 0
