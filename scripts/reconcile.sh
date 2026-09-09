#!/usr/bin/env bash
# ChangeLake Phase 8 / G9 — Source ↔ Lake reconcile (current-state demo check).
#
# Compares MySQL vs Paimon ODS (preferred):
#   - table row counts: users ↔ ods_users, orders ↔ ods_orders (+ order_items)
#   - SUM(amount) total + by dt + by dt+channel (DECIMAL, tolerance 0.01)
# Optionally (cheap) also DWD count/sum and a note that ADS uses 'unknown' for NULL.
#
# Report: human table + CSV/JSON as source_reconcile_report under docs/evidence/
# Any FAIL → exit 2. Success → "[G9] PASS reconcile"
#
# Honest: NOT continuous monitoring / NOT EO-2PC / NOT Exactly-Once E2E.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/scripts/common.sh"

EVIDENCE_DIR="$ROOT/docs/evidence"
REPORTS_DIR="$ROOT/reports"
mkdir -p "$EVIDENCE_DIR" "$REPORTS_DIR"

EVIDENCE_FILE="$EVIDENCE_DIR/g9_reconcile.txt"
CSV_REPORT="$EVIDENCE_DIR/source_reconcile_report.csv"
JSON_REPORT="$EVIDENCE_DIR/source_reconcile_report.json"
# Mirror under reports/ for discoverability
CSV_REPORT_MIRROR="$REPORTS_DIR/source_reconcile_report.csv"
JSON_REPORT_MIRROR="$REPORTS_DIR/source_reconcile_report.json"

AMOUNT_TOLERANCE="${AMOUNT_TOLERANCE:-0.01}"
RECONCILE_INCLUDE_DWD="${RECONCILE_INCLUDE_DWD:-auto}"  # auto|yes|no
JSONL="$(mktemp)"
trap 'rm -f "$JSONL"' EXIT

fail() {
  echo "[G9] FAIL $*"
  {
    echo "G9 Reconcile / Phase 8"
    echo "FAIL: $*"
    echo "tolerance=${AMOUNT_TOLERANCE}"
    echo "NOT claimed: continuous monitoring / EO-2PC / Exactly-Once E2E / G10 compaction"
    if [[ -f "$CSV_REPORT" ]]; then
      echo "partial_csv=${CSV_REPORT}"
    fi
  } >"$EVIDENCE_FILE"
  exit 2
}

emit_metric() {
  # emit_metric <metric> <kind> <source> <lake>
  local metric="$1" kind="$2" source="$3" lake="$4"
  python3 -c '
import json,sys
print(json.dumps({"metric":sys.argv[1],"kind":sys.argv[2],"source":sys.argv[3],"lake":sys.argv[4]}, separators=(",",":")))
' "$metric" "$kind" "$source" "$lake" >>"$JSONL"
}

mysql_tsv() {
  docker compose exec -T mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" -N -e "$1" "$MYSQL_DATABASE" | tr -d '\r'
}

parse_tableau_rows() {
  # Parse Flink tableau into TSV-ish lines (cells joined by TAB). stdin → stdout.
  python3 -c '
import re,sys
for line in sys.stdin:
    if "|" not in line:
        continue
    s=line.strip()
    if set(s)<=set("+-| "):
        continue
    low=s.lower()
    if "row" in low and "set" in low:
        continue
    cells=[x.strip() for x in line.split("|")]
    if cells and cells[0]=="": cells=cells[1:]
    if cells and cells[-1]=="": cells=cells[:-1]
    cells=[c.strip() for c in cells]
    if not cells:
        continue
    # skip header rows
    joined=" ".join(cells).lower()
    if any(h in joined for h in ("order_id","user_id","dt ","channel","amount","count")) and not re.fullmatch(r"-?\d+(\.\d+)?", cells[0] if cells else ""):
        # allow dt-looking first cell
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", cells[0]):
            if cells[0].lower() in ("dt","channel","metric","expr$0","count(1)","count(*)","sum(amount)"):
                continue
            if not re.fullmatch(r"-?\d+(\.\d+)?", cells[-1] if cells else ""):
                continue
    print("\t".join(cells))
'
}

echo "=================================================="
echo "[G9] Reconcile / Phase 8"
echo "=================================================="
echo "[g9] amount tolerance=${AMOUNT_TOLERANCE} (DECIMAL)"
echo "[g9] preferred path: MySQL ↔ ODS (current-state)"
echo "[g9] ODS channel: NULL kept as NULL (ADS 'unknown' only for ADS layer)"

# ---- Preconditions ----
bash "$ROOT/scripts/wait_services.sh" >/dev/null || fail "services not healthy"

ch_mysql="$(mysql_scalar "
SELECT COUNT(*) FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA='changelake' AND TABLE_NAME='orders' AND COLUMN_NAME='channel';
")" || ch_mysql=0

ensure_evolved_pipeline() {
  # Do NOT call start_pipeline.sh when channel exists — baseline SQL DROPs ODS.
  echo "[g9] ensuring evolved CDC job (channel present; no ODS DROP)"
  if ! paimon_column_exists ods ods_orders channel; then
    # Cold path: need a baseline job briefly so schema_evolution can ALTER+resubmit,
    # OR submit evolved SQL if ODS already has channel-shaped table from prior run.
    echo "[g9] ods.ods_orders.channel missing — starting baseline then schema_evolution"
    bash "$ROOT/scripts/start_pipeline.sh" || fail "start_pipeline.sh failed"
    bash "$ROOT/scripts/schema_evolution.sh" || fail "schema_evolution.sh failed"
  else
    cancel_flink_jobs "$PIPELINE_JOB_NAME" || true
    sleep 2
    docker compose exec -d jobmanager ./bin/sql-client.sh -f /opt/flink/sql-changelake/submit_ods_pipeline_evolved.sql
    wait_flink_job_running "$PIPELINE_JOB_NAME" "${CDC_WAIT_TIMEOUT}"       || fail "evolved pipeline did not reach RUNNING"
  fi
}

if ! flink_job_is_running "$PIPELINE_JOB_NAME"; then
  if [[ "$ch_mysql" == "1" ]]; then
    ensure_evolved_pipeline
  else
    echo "[g9] ODS pipeline not RUNNING — start_pipeline.sh (baseline, no channel)"
    bash "$ROOT/scripts/start_pipeline.sh" || fail "start_pipeline.sh failed"
  fi
else
  # Pipeline RUNNING: if MySQL has channel but ODS job/table does not, evolve in place.
  if [[ "$ch_mysql" == "1" ]] && ! paimon_column_exists ods ods_orders channel; then
    echo "[g9] MySQL has channel but ODS does not — running schema_evolution.sh"
    bash "$ROOT/scripts/schema_evolution.sh" || fail "schema_evolution.sh failed"
  fi
fi

if ! flink_job_is_running "$PIPELINE_JOB_NAME"; then
  fail "ODS pipeline not RUNNING"
fi

# Wait for ODS counts to catch MySQL (current-state lag gate; still a demo check)
SRC_USERS="$(mysql_scalar "SELECT COUNT(*) FROM users;")" || fail "MySQL users count failed"
SRC_ORDERS="$(mysql_scalar "SELECT COUNT(*) FROM orders;")" || fail "MySQL orders count failed"
SRC_ITEMS="$(mysql_scalar "SELECT COUNT(*) FROM order_items;")" || fail "MySQL order_items count failed"

catchup_ok=0
deadline=$((SECONDS + CDC_WAIT_TIMEOUT))
echo "[g9] waiting for ODS counts == MySQL (timeout=${CDC_WAIT_TIMEOUT}s)"
while (( SECONDS < deadline )); do
  LAKE_USERS="$(ods_count ods_users 2>/dev/null || echo '')"
  LAKE_ORDERS="$(ods_count ods_orders 2>/dev/null || echo '')"
  LAKE_ITEMS="$(ods_count ods_order_items 2>/dev/null || echo '')"
  echo "[g9] probe users=${LAKE_USERS}/${SRC_USERS} orders=${LAKE_ORDERS}/${SRC_ORDERS} items=${LAKE_ITEMS}/${SRC_ITEMS}"
  if [[ "$LAKE_USERS" == "$SRC_USERS" && "$LAKE_ORDERS" == "$SRC_ORDERS" && "$LAKE_ITEMS" == "$SRC_ITEMS" ]]; then
    catchup_ok=1
    break
  fi
  sleep "$CDC_POLL_INTERVAL"
done
if (( catchup_ok != 1 )); then
  echo "[g9] WARN: count catch-up timeout — continuing to report (will FAIL on mismatch)"
fi

# ==================================================
# 1) Table-level row counts
# ==================================================
echo "[g9] collecting table counts (MySQL ↔ ODS)"

LAKE_USERS="$(ods_count ods_users)" || fail "ODS ods_users count failed"
LAKE_ORDERS="$(ods_count ods_orders)" || fail "ODS ods_orders count failed"
LAKE_ITEMS="$(ods_count ods_order_items)" || fail "ODS ods_order_items count failed"

echo "[g9] users:  source=${SRC_USERS} lake=${LAKE_USERS}"
echo "[g9] orders: source=${SRC_ORDERS} lake=${LAKE_ORDERS}"
echo "[g9] items:  source=${SRC_ITEMS} lake=${LAKE_ITEMS}"

emit_metric "users.count" count "$SRC_USERS" "$LAKE_USERS"
emit_metric "orders.count" count "$SRC_ORDERS" "$LAKE_ORDERS"
emit_metric "order_items.count" count "$SRC_ITEMS" "$LAKE_ITEMS"

# ==================================================
# 2) Amount total (DECIMAL)
# ==================================================
echo "[g9] collecting SUM(amount) totals"

SRC_AMT_TOTAL="$(mysql_scalar "SELECT COALESCE(ROUND(SUM(amount),2),0) FROM orders;")" \
  || fail "MySQL SUM(amount) failed"
LAKE_AMT_TOTAL="$(paimon_sql <<'SQL' | extract_plain_scalar
SELECT CAST(COALESCE(SUM(amount), 0) AS STRING) FROM ods.ods_orders;
SQL
)" || fail "ODS SUM(amount) failed"

echo "[g9] orders.amount.total: source=${SRC_AMT_TOTAL} lake=${LAKE_AMT_TOTAL}"
emit_metric "orders.amount.total" amount "$SRC_AMT_TOTAL" "$LAKE_AMT_TOTAL"

# ==================================================
# 3) Amount by dt
# ==================================================
echo "[g9] collecting SUM(amount) by dt"

mysql_by_dt="$(mysql_tsv "
SELECT DATE(order_ts) AS dt, COALESCE(ROUND(SUM(amount),2),0) AS amt
FROM orders
GROUP BY DATE(order_ts)
ORDER BY dt;
")" || fail "MySQL amount-by-dt failed"

lake_by_dt_raw="$(paimon_sql <<'SQL'
SELECT CAST(CAST(order_ts AS DATE) AS STRING) AS dt,
       CAST(COALESCE(SUM(amount), 0) AS STRING) AS amt
FROM ods.ods_orders
GROUP BY CAST(order_ts AS DATE)
ORDER BY dt;
SQL
)" || fail "ODS amount-by-dt failed"
lake_by_dt="$(echo "$lake_by_dt_raw" | parse_tableau_rows)"

# Merge dt keys via Python (Decimal-safe emit already deferred to report)
python3 - "$mysql_by_dt" "$lake_by_dt" "$JSONL" <<'PY' || fail "merge amount-by-dt failed"
import json, sys
from collections import OrderedDict

mysql_text, lake_text, path = sys.argv[1], sys.argv[2], sys.argv[3]

def parse_pairs(text):
    out = OrderedDict()
    for line in text.strip().splitlines():
        line=line.strip()
        if not line:
            continue
        parts=line.split("\t") if "\t" in line else line.split()
        if len(parts) < 2:
            continue
        dt, amt = parts[0], parts[1]
        if len(dt) == 10 and dt[4]=="-" and dt[7]=="-":
            out[dt] = amt
    return out

src = parse_pairs(mysql_text)
lake = parse_pairs(lake_text)
keys = sorted(set(src) | set(lake))
if not keys:
    print("ERROR: no dt keys", file=sys.stderr)
    sys.exit(1)
with open(path, "a", encoding="utf-8") as f:
    for dt in keys:
        s = src.get(dt, "0")
        l = lake.get(dt, "0")
        # Missing side → 0 (will FAIL if the other side non-zero)
        row = {"metric": f"orders.amount.dt={dt}", "kind": "amount", "source": s, "lake": l}
        f.write(json.dumps(row, separators=(",", ":")) + "\n")
        print(f"[g9] amount.dt={dt}: source={s} lake={l}")
PY

# ==================================================
# 4) Amount by dt + channel (NULL as-is for ODS)
# ==================================================
channel_col_cnt="$(mysql_scalar "
SELECT COUNT(*) FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA='changelake' AND TABLE_NAME='orders' AND COLUMN_NAME='channel';
")" || channel_col_cnt=0

ods_has_channel=0
if paimon_column_exists ods ods_orders channel; then
  ods_has_channel=1
fi

if [[ "$channel_col_cnt" == "1" && "$ods_has_channel" == "1" ]]; then
  echo "[g9] collecting SUM(amount) by dt+channel (NULL kept as NULL / sentinel __NULL__)"

  # MySQL: use CHAR(0) sentinel in SQL result — emit as NULL label
  mysql_by_dt_ch="$(mysql_tsv "
SELECT DATE(order_ts) AS dt,
       IF(channel IS NULL, '__NULL__', channel) AS ch,
       COALESCE(ROUND(SUM(amount),2),0) AS amt
FROM orders
GROUP BY DATE(order_ts), channel
ORDER BY dt, ch;
")" || fail "MySQL amount-by-dt-channel failed"

  lake_by_dt_ch_raw="$(paimon_sql <<'SQL'
SELECT CAST(CAST(order_ts AS DATE) AS STRING) AS dt,
       CASE WHEN channel IS NULL THEN '__NULL__' ELSE CAST(channel AS STRING) END AS ch,
       CAST(COALESCE(SUM(amount), 0) AS STRING) AS amt
FROM ods.ods_orders
GROUP BY CAST(order_ts AS DATE), channel
ORDER BY dt, ch;
SQL
)" || fail "ODS amount-by-dt-channel failed"
  lake_by_dt_ch="$(echo "$lake_by_dt_ch_raw" | parse_tableau_rows)"

  python3 - "$mysql_by_dt_ch" "$lake_by_dt_ch" "$JSONL" <<'PY' || fail "merge amount-by-dt-channel failed"
import json, sys
from collections import OrderedDict

mysql_text, lake_text, path = sys.argv[1], sys.argv[2], sys.argv[3]

def parse_triples(text):
    out = OrderedDict()
    for line in text.strip().splitlines():
        line=line.strip()
        if not line:
            continue
        parts=line.split("\t") if "\t" in line else line.split()
        if len(parts) < 3:
            continue
        dt, ch, amt = parts[0], parts[1], parts[2]
        if not (len(dt)==10 and dt[4]=="-" and dt[7]=="-"):
            continue
        out[(dt, ch)] = amt
    return out

src = parse_triples(mysql_text)
lake = parse_triples(lake_text)
keys = sorted(set(src) | set(lake), key=lambda x: (x[0], x[1]))
if not keys:
    print("ERROR: no dt+channel keys", file=sys.stderr)
    sys.exit(1)
with open(path, "a", encoding="utf-8") as f:
    for dt, ch in keys:
        s = src.get((dt, ch), "0")
        l = lake.get((dt, ch), "0")
        label = "NULL" if ch == "__NULL__" else ch
        metric = f"orders.amount.dt={dt}.channel={label}"
        row = {"metric": metric, "kind": "amount", "source": s, "lake": l}
        f.write(json.dumps(row, separators=(",", ":")) + "\n")
        print(f"[g9] amount.dt={dt}.channel={label}: source={s} lake={l}")
PY
else
  echo "[g9] skip amount-by-channel (MySQL channel col=${channel_col_cnt}, ODS channel=${ods_has_channel})"
  echo "[g9] note: after G5 both sides should have channel; run schema_evolution / golden path first"
fi

# ==================================================
# 5) Optional cheap DWD checks
# ==================================================
include_dwd="$RECONCILE_INCLUDE_DWD"
if [[ "$include_dwd" == "auto" ]]; then
  dwd_probe="$(paimon_sql <<'SQL' | extract_plain_scalar
SELECT COUNT(*) FROM dwd.dwd_orders;
SQL
  )" 2>/dev/null || dwd_probe=""
  if [[ -n "$dwd_probe" && "$dwd_probe" != "0" ]]; then
    include_dwd=yes
  else
    include_dwd=no
  fi
fi

if [[ "$include_dwd" == "yes" ]]; then
  echo "[g9] optional DWD reconcile (count + SUM(amount/net_amount))"
  DWD_CNT="$(paimon_sql <<'SQL' | extract_plain_scalar
SELECT COUNT(*) FROM dwd.dwd_orders;
SQL
  )" || fail "DWD count failed"
  DWD_AMT="$(paimon_sql <<'SQL' | extract_plain_scalar
SELECT CAST(COALESCE(SUM(amount), 0) AS STRING) FROM dwd.dwd_orders;
SQL
  )" || fail "DWD SUM(amount) failed"
  DWD_NET="$(paimon_sql <<'SQL' | extract_plain_scalar
SELECT CAST(COALESCE(SUM(net_amount), 0) AS STRING) FROM dwd.dwd_orders;
SQL
  )" || fail "DWD SUM(net_amount) failed"
  emit_metric "dwd.orders.count_vs_mysql" count "$SRC_ORDERS" "$DWD_CNT"
  emit_metric "dwd.orders.amount.total_vs_mysql" amount "$SRC_AMT_TOTAL" "$DWD_AMT"
  emit_metric "dwd.orders.net_amount.total_vs_mysql" amount "$SRC_AMT_TOTAL" "$DWD_NET"
  echo "[g9] dwd: count=${DWD_CNT} amount=${DWD_AMT} net=${DWD_NET}"
else
  echo "[g9] skip optional DWD (table empty or RECONCILE_INCLUDE_DWD=no)"
fi

# ==================================================
# 6) Format report (DECIMAL compare) + exit gate
# ==================================================
echo
echo "[g9] writing source_reconcile_report (CSV + JSON + human table)"
report_rc=0
python3 "$ROOT/python/reconcile_report.py" \
  --csv "$CSV_REPORT" \
  --json "$JSON_REPORT" \
  --tolerance "$AMOUNT_TOLERANCE" \
  --title "source_reconcile_report" \
  <"$JSONL" || report_rc=$?

cp -f "$CSV_REPORT" "$CSV_REPORT_MIRROR"
cp -f "$JSON_REPORT" "$JSON_REPORT_MIRROR"

OVERALL="$(python3 -c "import json; print(json.load(open('$JSON_REPORT'))['overall'])")"

{
  echo "G9 Reconcile / Phase 8"
  echo "path=MySQL ↔ Paimon ODS (current-state); optional DWD if present"
  echo "tolerance=${AMOUNT_TOLERANCE}"
  echo "decimal_only=yes (python decimal.Decimal; no float money compare)"
  echo "channel_policy_ods=NULL as-is (sentinel __NULL__ in grouping; report label NULL)"
  echo "channel_policy_ads=unknown is ADS-only (Phase 5); not remapped for ODS metrics"
  echo "csv=${CSV_REPORT}"
  echo "json=${JSON_REPORT}"
  echo "mirror_csv=${CSV_REPORT_MIRROR}"
  echo "mirror_json=${JSON_REPORT_MIRROR}"
  echo "users.source=${SRC_USERS} lake=${LAKE_USERS}"
  echo "orders.source=${SRC_ORDERS} lake=${LAKE_ORDERS}"
  echo "order_items.source=${SRC_ITEMS} lake=${LAKE_ITEMS}"
  echo "orders.amount.total.source=${SRC_AMT_TOTAL} lake=${LAKE_AMT_TOTAL}"
  echo "include_dwd=${include_dwd}"
  echo "overall=${OVERALL}"
  echo "NOT claimed: continuous monitoring / EO-2PC / Exactly-Once E2E / G10 compaction / production SLA"
  if [[ "$OVERALL" == "PASS" && "$report_rc" == "0" ]]; then
    echo "PASS"
  else
    echo "FAIL"
  fi
} >"$EVIDENCE_FILE"

if [[ "$report_rc" != "0" || "$OVERALL" != "PASS" ]]; then
  echo
  echo "[G9] FAIL reconcile (see ${CSV_REPORT} / ${EVIDENCE_FILE})"
  exit 2
fi

echo
echo "[G9] PASS reconcile"
echo "[g9] evidence: ${EVIDENCE_FILE}"
echo "[g9] report:   ${CSV_REPORT}"
echo "[g9] report:   ${JSON_REPORT}"
exit 0
