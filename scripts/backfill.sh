#!/usr/bin/env bash
# ChangeLake Phase 6 / G7: date-scoped idempotent backfill.
# Usage: bash scripts/backfill.sh <dt>   OR   make backfill DT=YYYY-MM-DD
#
# Flow (MySQL = source of truth for the day; CDC not required):
#   1) Snapshot MySQL orders WHERE DATE(order_ts)=dt
#   2) DELETE DWD rows for that dt → INSERT from MySQL snapshot (net_amount=amount)
#   3) DELETE ADS rows for that dt → INSERT aggregates from DWD for dt
#   4) Reconcile MySQL vs DWD/ADS for dt
#   5) Print content fingerprint (canonical sort + SHA256)
#
# Honest: demo partition-scoped repair (logical dt), not EO-2PC / not a general orchestrator.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/scripts/common.sh"

DT="${1:-${DT:-}}"
if [[ -z "$DT" ]]; then
  echo "Usage: bash scripts/backfill.sh <YYYY-MM-DD>" >&2
  echo "   or: make backfill DT=YYYY-MM-DD" >&2
  exit 2
fi
if ! [[ "$DT" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "[backfill] ERROR: dt must be YYYY-MM-DD, got: ${DT}" >&2
  exit 2
fi

PAID_STATUSES="'paid','shipped','completed'"
FP_PY="$ROOT/python/fingerprint.py"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

echo "=================================================="
echo "[backfill] Phase 6 / G7 dt-scoped repair"
echo "=================================================="
echo "[backfill] DT=${DT}"
echo "[backfill] paid statuses: (${PAID_STATUSES})"
echo "[backfill] NULL channel → ADS literal 'unknown'"
echo "[backfill] source of truth: MySQL (bypass CDC)"

# ---- helpers ----
mysql_has_channel() {
  local cnt
  cnt="$(mysql_scalar "
SELECT COUNT(*) FROM information_schema.COLUMNS
WHERE TABLE_SCHEMA='changelake' AND TABLE_NAME='orders' AND COLUMN_NAME='channel';
")" || cnt=0
  [[ "$cnt" == "1" ]]
}

submit_sql_file() {
  local host_sql="$1"
  local label="$2"
  docker compose cp "$host_sql" jobmanager:/tmp/changelake_backfill.sql >/dev/null
  echo "[backfill] submitting ${label}"
  if ! docker compose exec -T jobmanager ./bin/sql-client.sh -f /tmp/changelake_backfill.sql \
      >"${TMP_DIR}/${label}.out" 2>&1; then
    echo "[backfill] FAIL: ${label} sql-client non-zero" >&2
    cat "${TMP_DIR}/${label}.out" >&2 || true
    exit 2
  fi
  if grep -q '\[ERROR\]' "${TMP_DIR}/${label}.out"; then
    echo "[backfill] FAIL: ${label} statement error" >&2
    cat "${TMP_DIR}/${label}.out" >&2 || true
    exit 2
  fi
}

# ---- 1) MySQL snapshot for dt ----
echo "[backfill] snapshotting MySQL orders for dt=${DT}"
HAS_CHANNEL=0
if mysql_has_channel; then
  HAS_CHANNEL=1
  echo "[backfill] MySQL orders.channel present — included in snapshot"
  SNAP_SQL="
SELECT order_id, user_id, status,
       CAST(amount AS CHAR),
       COALESCE(channel, ''),
       DATE_FORMAT(order_ts, '%Y-%m-%d %H:%i:%s'),
       DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%s')
FROM orders
WHERE DATE(order_ts) = '${DT}'
ORDER BY order_id;
"
else
  echo "[backfill] MySQL orders.channel absent — DWD channel=NULL"
  SNAP_SQL="
SELECT order_id, user_id, status,
       CAST(amount AS CHAR),
       '',
       DATE_FORMAT(order_ts, '%Y-%m-%d %H:%i:%s'),
       DATE_FORMAT(updated_at, '%Y-%m-%d %H:%i:%s')
FROM orders
WHERE DATE(order_ts) = '${DT}'
ORDER BY order_id;
"
fi

# TSV: order_id user_id status amount channel order_ts updated_at
mysql_exec -N -e "$SNAP_SQL" >"${TMP_DIR}/mysql_snap.tsv" || {
  echo "[backfill] FAIL: MySQL snapshot query failed" >&2
  exit 2
}

MYSQL_CNT="$(wc -l <"${TMP_DIR}/mysql_snap.tsv" | tr -d ' ')"
echo "[backfill] MySQL rows for dt=${DT}: ${MYSQL_CNT}"
if [[ "$MYSQL_CNT" == "0" ]]; then
  echo "[backfill] WARN: no MySQL orders for dt=${DT} — will clear DWD/ADS for that dt only"
fi

# ---- 2) Build DWD DELETE + INSERT SQL ----
{
  sed "s/__DT__/${DT}/g" "$ROOT/flink/sql/backfill_dwd_dt.sql.tpl" \
    | grep -v '__DWD_VALUES_INSERT__'
  if [[ "$MYSQL_CNT" != "0" ]]; then
    echo "INSERT INTO dwd.dwd_orders ("
    echo "  order_id, user_id, status, amount, channel, coupon_amount, net_amount, order_ts, updated_at"
    echo ") VALUES"
    python3 - "$TMP_DIR/mysql_snap.tsv" <<'PY'
import sys
path = sys.argv[1]
rows = []
with open(path, encoding="utf-8") as f:
    for line in f:
        line = line.strip("\r\n")
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 7:
            print(f"bad snap row: {line!r}", file=sys.stderr)
            sys.exit(1)
        oid, uid, status, amount, channel, order_ts, updated_at = parts[:7]
        status_sql = status.replace("'", "''")
        if channel.strip() == "":
            ch_sql = "CAST(NULL AS STRING)"
        else:
            ch_sql = "'" + channel.replace("'", "''") + "'"
        rows.append(
            f"  ({oid}, {uid}, '{status_sql}', CAST({amount} AS DECIMAL(12,2)), "
            f"{ch_sql}, CAST(NULL AS DECIMAL(12,2)), CAST({amount} AS DECIMAL(12,2)), "
            f"TIMESTAMP '{order_ts}', TIMESTAMP '{updated_at}')"
        )
print(",\n".join(rows) + ";")
PY
  else
    echo "-- no MySQL rows; DELETE alone is the repair"
  fi
} >"${TMP_DIR}/backfill_dwd.sql"

# Cancel ADS + streaming DWD so batch DELETE/INSERT can schedule without races.
# Backfill does not require CDC/DWD streaming; MySQL is source of truth for this dt.
cancel_flink_jobs "${ADS_JOB_NAME:-changelake-ads-order-daily}" || true
cancel_flink_jobs "${DWD_JOB_NAME:-changelake-dwd-orders}" || true
cancel_flink_jobs "changelake-backfill-dwd" || true
cancel_flink_jobs "changelake-backfill-ads" || true
sleep 2

submit_sql_file "${TMP_DIR}/backfill_dwd.sql" "dwd_dt_${DT}"

# ---- 3) ADS rebuild for dt ----
sed "s/__DT__/${DT}/g" "$ROOT/flink/sql/backfill_ads_dt.sql.tpl" \
  >"${TMP_DIR}/backfill_ads.sql"
submit_sql_file "${TMP_DIR}/backfill_ads.sql" "ads_dt_${DT}"

# ---- 4) Reconcile MySQL vs DWD / ADS ----
echo "[backfill] reconciling MySQL ↔ DWD/ADS for dt=${DT}"

mysql_amt="$(mysql_scalar "SELECT COALESCE(ROUND(SUM(amount),2),0) FROM orders WHERE DATE(order_ts)='${DT}';")" \
  || { echo "[backfill] FAIL: MySQL amount sum"; exit 2; }
mysql_cnt_chk="$(mysql_scalar "SELECT COUNT(*) FROM orders WHERE DATE(order_ts)='${DT}';")" \
  || { echo "[backfill] FAIL: MySQL count"; exit 2; }

dwd_cnt="$(paimon_sql <<SQL | extract_plain_scalar
SELECT COUNT(*) FROM dwd.dwd_orders WHERE CAST(order_ts AS DATE) = DATE '${DT}';
SQL
)" || { echo "[backfill] FAIL: DWD count query"; exit 2; }

dwd_net="$(paimon_sql <<SQL | extract_plain_scalar
SELECT CAST(COALESCE(SUM(net_amount), 0) AS STRING)
FROM dwd.dwd_orders WHERE CAST(order_ts AS DATE) = DATE '${DT}';
SQL
)" || { echo "[backfill] FAIL: DWD net sum"; exit 2; }

python3 -c "
import sys
mc, dc = int(sys.argv[1]), int(float(sys.argv[2]))
sys.exit(0 if mc == dc else 1)
" "$mysql_cnt_chk" "$dwd_cnt" || {
  echo "[backfill] FAIL: DWD count=${dwd_cnt} != MySQL count=${mysql_cnt_chk} for dt=${DT}" >&2
  exit 2
}

python3 -c "
import sys
a,b=float(sys.argv[1]),float(sys.argv[2])
sys.exit(0 if abs(a-b)<0.001 else 1)
" "$mysql_amt" "$dwd_net" || {
  echo "[backfill] FAIL: DWD SUM(net_amount)=${dwd_net} != MySQL SUM(amount)=${mysql_amt}" >&2
  exit 2
}
echo "[backfill] DWD reconcile OK: count=${dwd_cnt} sum(net_amount)=${dwd_net}"

# ADS vs MySQL expectations (same formulas as Phase 5)
if (( HAS_CHANNEL == 1 )); then
  mysql_ads_sql="
SELECT COALESCE(channel, 'unknown') AS channel,
       COUNT(*) AS order_cnt,
       SUM(CASE WHEN status IN (${PAID_STATUSES}) THEN 1 ELSE 0 END) AS paid_order_cnt,
       ROUND(SUM(CASE WHEN status IN (${PAID_STATUSES}) THEN amount ELSE 0 END), 2) AS gmv,
       ROUND(SUM(CASE WHEN status IN (${PAID_STATUSES}) THEN amount ELSE 0 END), 2) AS net_gmv,
       COUNT(DISTINCT CASE WHEN status IN (${PAID_STATUSES}) THEN user_id END) AS buyer_cnt
FROM orders
WHERE DATE(order_ts)='${DT}'
GROUP BY COALESCE(channel, 'unknown')
ORDER BY channel;
"
else
  mysql_ads_sql="
SELECT 'unknown' AS channel,
       COUNT(*) AS order_cnt,
       SUM(CASE WHEN status IN (${PAID_STATUSES}) THEN 1 ELSE 0 END) AS paid_order_cnt,
       ROUND(SUM(CASE WHEN status IN (${PAID_STATUSES}) THEN amount ELSE 0 END), 2) AS gmv,
       ROUND(SUM(CASE WHEN status IN (${PAID_STATUSES}) THEN amount ELSE 0 END), 2) AS net_gmv,
       COUNT(DISTINCT CASE WHEN status IN (${PAID_STATUSES}) THEN user_id END) AS buyer_cnt
FROM orders
WHERE DATE(order_ts)='${DT}'
GROUP BY 1
ORDER BY channel;
"
fi

mysql_exec -N -e "$mysql_ads_sql" >"${TMP_DIR}/mysql_ads.tsv" || {
  echo "[backfill] FAIL: MySQL ADS expectation query" >&2
  exit 2
}

ads_raw="$(paimon_sql <<SQL
SELECT COALESCE(channel, 'unknown') AS channel,
       CAST(order_cnt AS STRING) AS order_cnt,
       CAST(paid_order_cnt AS STRING) AS paid_order_cnt,
       CAST(gmv AS STRING) AS gmv,
       CAST(net_gmv AS STRING) AS net_gmv,
       CAST(buyer_cnt AS STRING) AS buyer_cnt
FROM ads.ads_order_daily
WHERE dt = DATE '${DT}'
ORDER BY channel;
SQL
)" || { echo "[backfill] FAIL: ADS query"; exit 2; }
printf '%s
' "$ads_raw" >"${TMP_DIR}/ads_raw.txt"

python3 - "${TMP_DIR}/mysql_ads.tsv" "${TMP_DIR}/ads_raw.txt" <<'PY' || {
import re, sys

mysql_path = sys.argv[1]
ads_text = open(sys.argv[2], encoding="utf-8").read()

def parse_mysql(path):
    rows = {}
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip("\r\n")
            if not line.strip():
                continue
            parts = line.split("\t")
            if len(parts) < 6:
                continue
            ch, oc, poc, gmv, ngmv, bc = parts[:6]
            rows[ch] = {
                "order_cnt": int(float(oc)),
                "paid_order_cnt": int(float(poc)),
                "gmv": round(float(gmv), 2),
                "net_gmv": round(float(ngmv), 2),
                "buyer_cnt": int(float(bc)),
            }
    return rows

def parse_ads(text):
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
        ch, oc, poc, gmv, ngmv, bc = cells[:6]
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

exp = parse_mysql(mysql_path)
got = parse_ads(ads_text)
# Empty MySQL day → ADS for dt must also be empty
if not exp:
    if got:
        print(f"EXTRA ADS channels on empty MySQL day: {sorted(got)}", file=sys.stderr)
        sys.exit(1)
    print("ADS empty OK (no MySQL rows)")
    sys.exit(0)
ok = True
for ch, e in sorted(exp.items()):
    g = got.get(ch)
    if g is None:
        print(f"MISSING ADS channel={ch}", file=sys.stderr)
        ok = False
        continue
    for k in e:
        if e[k] != g.get(k):
            print(f"MISMATCH channel={ch} {k}: expected={e[k]} got={g.get(k)}", file=sys.stderr)
            ok = False
    if all(e[k] == g.get(k) for k in e):
        print(f"MATCH channel={ch} {e}")
extra = set(got) - set(exp)
if extra:
    print(f"EXTRA ADS channels: {sorted(extra)}", file=sys.stderr)
    ok = False
sys.exit(0 if ok else 1)
PY
  echo "[backfill] FAIL: ADS metrics != MySQL for dt=${DT}" >&2
  echo "$ads_raw" >&2
  exit 2
}

echo "[backfill] ADS reconcile OK for dt=${DT}"

# ---- 5) Fingerprint (content-only) ----
dwd_dump="$(paimon_sql <<SQL
SELECT order_id, user_id, status,
       CAST(amount AS STRING) AS amount,
       CASE WHEN channel IS NULL THEN 'NULL' ELSE CAST(channel AS STRING) END AS channel,
       CAST(coupon_amount AS STRING) AS coupon_amount,
       CAST(net_amount AS STRING) AS net_amount
FROM dwd.dwd_orders
WHERE CAST(order_ts AS DATE) = DATE '${DT}'
ORDER BY order_id;
SQL
)" || { echo "[backfill] FAIL: DWD dump for fingerprint"; exit 2; }

ads_dump="$(paimon_sql <<SQL
SELECT CAST(dt AS STRING) AS dt,
       COALESCE(channel, 'unknown') AS channel,
       CAST(order_cnt AS STRING) AS order_cnt,
       CAST(paid_order_cnt AS STRING) AS paid_order_cnt,
       CAST(gmv AS STRING) AS gmv,
       CAST(net_gmv AS STRING) AS net_gmv,
       CAST(buyer_cnt AS STRING) AS buyer_cnt
FROM ads.ads_order_daily
WHERE dt = DATE '${DT}'
ORDER BY channel;
SQL
)" || { echo "[backfill] FAIL: ADS dump for fingerprint"; exit 2; }

DWD_SHA="$(printf '%s\n' "$dwd_dump" | python3 "$FP_PY" --role dwd --dt "$DT")"
ADS_SHA="$(printf '%s\n' "$ads_dump" | python3 "$FP_PY" --role ads --dt "$DT")"
FP="$(python3 "$FP_PY" --combine "$DWD_SHA" "$ADS_SHA")"

echo "[backfill] fingerprint.dwd_sha256=${DWD_SHA}"
echo "[backfill] fingerprint.ads_sha256=${ADS_SHA}"
echo "[backfill] fingerprint=${FP}"
echo "[backfill] DONE dt=${DT}"

# Machine-readable last line for demo harness
echo "BACKFILL_FINGERPRINT=${FP}"
