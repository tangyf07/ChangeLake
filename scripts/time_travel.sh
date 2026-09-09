#!/usr/bin/env bash
# ChangeLake Phase 7 / G8: Paimon Snapshot / Time Travel demo.
#
# Creates ≥3 snapshots on a dedicated table ods.ods_tt_demo (order_id=100 story):
#   S1 INSERT amount=100 → S2 UPDATE amount=200 → S3 DELETE (row absent)
# Records snapshot_id + commit_time; queries each via scan.snapshot-id OPTIONS hint.
# Prints [G8] PASS time travel on success. Exit 2 on hard fail.
#
# Dedicated table: does NOT mutate ods_orders / golden-path seed counts.
# Honest: snapshot-id based demo; NOT EO-2PC; NOT G9–G10 / Phase 8+.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/scripts/common.sh"

EVIDENCE_DIR="$ROOT/docs/evidence"
EVIDENCE_FILE="$EVIDENCE_DIR/g8_time_travel.txt"
mkdir -p "$EVIDENCE_DIR"

DEMO_ORDER_ID="${TT_ORDER_ID:-100}"
S1_AMOUNT="100.00"
S2_AMOUNT="200.00"
TABLE="ods.ods_tt_demo"

fail() {
  echo "[G8] FAIL $*"
  {
    echo "G8 Time Travel / Phase 7"
    echo "table=${TABLE} order_id=${DEMO_ORDER_ID}"
    echo "FAIL: $*"
    echo "NOT claimed: EO-2PC / Exactly-Once E2E / G9–G10 / full reconcile / compaction"
  } >"$EVIDENCE_FILE"
  exit 2
}

# Dump $snapshots system table (raw).
dump_snapshots() {
  paimon_sql <<'SQL'
SELECT snapshot_id, CAST(commit_time AS STRING) AS commit_time
FROM ods.`ods_tt_demo$snapshots`
ORDER BY snapshot_id;
SQL
}

# Parse latest snapshot_id from dump_snapshots tableau output.
latest_snapshot_id() {
  dump_snapshots | python3 -c '
import re, sys
text = sys.stdin.read()
ids = []
for line in text.splitlines():
    if "|" not in line:
        continue
    s = line.strip()
    if set(s) <= set("+-| "):
        continue
    low = s.lower()
    if "row" in low and "set" in low:
        continue
    if "snapshot_id" in low:
        continue
    cells = [x.strip() for x in line.split("|") if x.strip() != ""]
    if not cells:
        continue
    if re.fullmatch(r"\d+", cells[0]):
        ids.append(int(cells[0]))
if not ids:
    sys.stderr.write("no snapshot_id parsed from:\n" + text + "\n")
    sys.exit(1)
print(max(ids))
'
}

# Return "snapshot_id|commit_time" for a specific snapshot_id.
snapshot_meta() {
  local sid="$1"
  dump_snapshots | python3 -c '
import re, sys
want = sys.argv[1]
text = sys.stdin.read()
for line in text.splitlines():
    if "|" not in line:
        continue
    s = line.strip()
    if set(s) <= set("+-| "):
        continue
    low = s.lower()
    if "row" in low and "set" in low:
        continue
    if "snapshot_id" in low:
        continue
    cells = [x.strip() for x in line.split("|") if x.strip() != ""]
    if len(cells) < 2:
        continue
    if cells[0] != want:
        continue
    commit = ""
    for c in cells[1:]:
        if re.search(r"\d{4}-\d{2}-\d{2}", c):
            commit = c
            break
    if not commit:
        commit = cells[1]
    print(f"{cells[0]}|{commit}")
    sys.exit(0)
sys.stderr.write(f"snapshot_id={want} not found in:\n" + text + "\n")
sys.exit(1)
' "$sid"
}

query_at_snapshot() {
  local sid="$1"
  paimon_sql <<SQL
SELECT order_id, CAST(amount AS STRING) AS amount, status
FROM ${TABLE} /*+ OPTIONS('scan.snapshot-id'='${sid}') */
WHERE order_id = ${DEMO_ORDER_ID};
SQL
}

query_current() {
  paimon_sql <<SQL
SELECT order_id, CAST(amount AS STRING) AS amount, status
FROM ${TABLE}
WHERE order_id = ${DEMO_ORDER_ID};
SQL
}

# True if query output has order_id and amount ≈ expect.
amount_present() {
  local expect="$1"
  local out="$2"
  python3 -c '
import re, sys
expect = float(sys.argv[1])
oid = float(sys.argv[2])
text = sys.argv[3]
rows = []
for line in text.splitlines():
    if "|" not in line:
        continue
    s = line.strip()
    if set(s) <= set("+-| "):
        continue
    low = s.lower()
    if "row" in low and "set" in low:
        continue
    if "order_id" in low and "amount" in low:
        continue
    cells = [x.strip() for x in line.split("|") if x.strip() != ""]
    nums = []
    for c in cells:
        if re.fullmatch(r"-?\d+(\.\d+)?", c):
            nums.append(float(c))
    if len(nums) >= 2:
        rows.append(nums)
# Prefer (order_id, amount, ...)
for nums in rows:
    if abs(nums[0] - oid) < 0.001 and abs(nums[1] - expect) < 0.001:
        sys.exit(0)
# Fallback: any row containing both values
for nums in rows:
    has_oid = any(abs(v - oid) < 0.001 for v in nums)
    has_amt = any(abs(v - expect) < 0.001 for v in nums)
    if has_oid and has_amt:
        sys.exit(0)
sys.exit(1)
' "$expect" "$DEMO_ORDER_ID" "$out"
}

# True if order_id is not present as a data row.
row_absent() {
  local out="$1"
  python3 -c '
import re, sys
oid = float(sys.argv[1])
text = sys.argv[2]
for line in text.splitlines():
    if "|" not in line:
        continue
    s = line.strip()
    if set(s) <= set("+-| "):
        continue
    low = s.lower()
    if "row" in low and "set" in low:
        continue
    if "order_id" in low and "amount" in low:
        continue
    cells = [x.strip() for x in line.split("|") if x.strip() != ""]
    nums = [float(c) for c in cells if re.fullmatch(r"-?\d+(\.\d+)?", c)]
    if nums and abs(nums[0] - oid) < 0.001:
        sys.exit(1)
sys.exit(0)
' "$DEMO_ORDER_ID" "$out"
}

echo "=================================================="
echo "[G8] Snapshot / Time Travel / Phase 7"
echo "=================================================="
echo "[g8] table=${TABLE} order_id=${DEMO_ORDER_ID}"
echo "[g8] story: S1 INSERT amount=${S1_AMOUNT} → S2 UPDATE amount=${S2_AMOUNT} → S3 DELETE"
echo "[g8] travel: /*+ OPTIONS('scan.snapshot-id'='N') */ (Paimon 1.4.2 + Flink 1.18)"
echo "[g8] dedicated demo table — does not touch ods_orders golden-path counts"

# ---- Create / reset dedicated demo table ----
echo "[g8] creating ${TABLE} (reset for idempotent re-runs)"
paimon_sql <<'SQL' >/dev/null || fail "CREATE/DROP ods.ods_tt_demo failed"
CREATE DATABASE IF NOT EXISTS ods;
DROP TABLE IF EXISTS ods.ods_tt_demo;
CREATE TABLE ods.ods_tt_demo (
  order_id BIGINT,
  amount DECIMAL(12, 2),
  status STRING,
  updated_at TIMESTAMP(0),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'bucket' = '1',
  'snapshot.num-retained.min' = '10',
  'snapshot.num-retained.max' = '50'
);
SQL

# ---- S1: INSERT ----
echo "[g8] S1 INSERT order_id=${DEMO_ORDER_ID} amount=${S1_AMOUNT}"
paimon_sql <<SQL >/dev/null || fail "S1 INSERT failed"
INSERT INTO ${TABLE} VALUES (
  ${DEMO_ORDER_ID},
  CAST(${S1_AMOUNT} AS DECIMAL(12,2)),
  'created',
  TIMESTAMP '2026-09-09 10:00:00'
);
SQL
sleep 1
S1_ID="$(latest_snapshot_id)" || fail "could not read snapshot_id after S1"
S1_META="$(snapshot_meta "$S1_ID")" || S1_META="${S1_ID}|"
S1_TIME="${S1_META#*|}"
echo "[g8] S1 snapshot_id=${S1_ID} commit_time=${S1_TIME}"

# ---- S2: UPDATE amount ----
echo "[g8] S2 UPDATE amount → ${S2_AMOUNT}"
paimon_sql <<SQL >/dev/null || fail "S2 UPDATE failed"
UPDATE ${TABLE}
SET amount = CAST(${S2_AMOUNT} AS DECIMAL(12,2)),
    status = 'paid',
    updated_at = TIMESTAMP '2026-09-09 11:00:00'
WHERE order_id = ${DEMO_ORDER_ID};
SQL
sleep 1
S2_ID="$(latest_snapshot_id)" || fail "could not read snapshot_id after S2"
S2_META="$(snapshot_meta "$S2_ID")" || S2_META="${S2_ID}|"
S2_TIME="${S2_META#*|}"
echo "[g8] S2 snapshot_id=${S2_ID} commit_time=${S2_TIME}"
if [[ "$S2_ID" == "$S1_ID" ]]; then
  fail "S2 snapshot_id equals S1 (${S1_ID}); expected a new snapshot after UPDATE"
fi

# ---- S3: DELETE ----
echo "[g8] S3 DELETE order_id=${DEMO_ORDER_ID}"
paimon_sql <<SQL >/dev/null || fail "S3 DELETE failed"
DELETE FROM ${TABLE} WHERE order_id = ${DEMO_ORDER_ID};
SQL
sleep 1
S3_ID="$(latest_snapshot_id)" || fail "could not read snapshot_id after S3"
S3_META="$(snapshot_meta "$S3_ID")" || S3_META="${S3_ID}|"
S3_TIME="${S3_META#*|}"
echo "[g8] S3 snapshot_id=${S3_ID} commit_time=${S3_TIME}"
if [[ "$S3_ID" == "$S2_ID" ]]; then
  fail "S3 snapshot_id equals S2 (${S2_ID}); expected a new snapshot after DELETE"
fi

# ---- Current-state query (should be absent after DELETE) ----
echo "[g8] Current State Query (latest snapshot) — expect row absent"
CUR="$(query_current)" || fail "current-state query failed"
echo "$CUR"
row_absent "$CUR" || fail "current-state still has order_id=${DEMO_ORDER_ID} after DELETE"

# ---- Historical Snapshot Queries ----
echo "[g8] Historical Snapshot Query S1 (scan.snapshot-id=${S1_ID}) — expect amount=${S1_AMOUNT}"
Q1="$(query_at_snapshot "$S1_ID")" || fail "S1 time-travel query failed"
echo "$Q1"
amount_present "100" "$Q1" || fail "S1 expected amount≈100 at snapshot ${S1_ID}"

echo "[g8] Historical Snapshot Query S2 (scan.snapshot-id=${S2_ID}) — expect amount=${S2_AMOUNT}"
Q2="$(query_at_snapshot "$S2_ID")" || fail "S2 time-travel query failed"
echo "$Q2"
amount_present "200" "$Q2" || fail "S2 expected amount≈200 at snapshot ${S2_ID}"

echo "[g8] Historical Snapshot Query S3 (scan.snapshot-id=${S3_ID}) — expect row absent"
Q3="$(query_at_snapshot "$S3_ID")" || fail "S3 time-travel query failed"
echo "$Q3"
row_absent "$Q3" || fail "S3 expected absence at snapshot ${S3_ID}"

# Optional: also demonstrate FOR SYSTEM_TIME AS OF using S2 commit_time when parseable
TT_NOTE="primary assertions use scan.snapshot-id (Flink OPTIONS hint)"
if [[ -n "$S2_TIME" && "$S2_TIME" =~ [0-9]{4}-[0-9]{2}-[0-9]{2} ]]; then
  TS_LIT="$(echo "$S2_TIME" | python3 -c '
import re,sys
t=sys.stdin.read().strip()
m=re.search(r"(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})", t)
print(f"{m.group(1)} {m.group(2)}" if m else "")
')"
  if [[ -n "$TS_LIT" ]]; then
    echo "[g8] optional FOR SYSTEM_TIME AS OF TIMESTAMP '${TS_LIT}' (around S2)"
    if QTS="$(paimon_sql <<SQL
SELECT order_id, CAST(amount AS STRING) AS amount, status
FROM ${TABLE} FOR SYSTEM_TIME AS OF TIMESTAMP '${TS_LIT}'
WHERE order_id = ${DEMO_ORDER_ID};
SQL
)"; then
      echo "$QTS"
      if amount_present "200" "$QTS" 2>/dev/null || amount_present "100" "$QTS" 2>/dev/null; then
        TT_NOTE="FOR SYSTEM_TIME AS OF TIMESTAMP '${TS_LIT}' also returned a historical row (best-effort)"
      else
        TT_NOTE="FOR SYSTEM_TIME attempted; G8 contract is scan.snapshot-id"
      fi
    else
      TT_NOTE="FOR SYSTEM_TIME AS OF skipped/failed; G8 contract is scan.snapshot-id"
    fi
  fi
fi

{
  echo "G8 Time Travel / Phase 7"
  echo "table=${TABLE}"
  echo "order_id=${DEMO_ORDER_ID}"
  echo "method=Paimon Flink SQL /*+ OPTIONS('scan.snapshot-id'='N') */"
  echo "optional_flink_118=${TT_NOTE}"
  echo "S1_action=INSERT amount=${S1_AMOUNT} status=created"
  echo "S1_snapshot_id=${S1_ID}"
  echo "S1_commit_time=${S1_TIME}"
  echo "S2_action=UPDATE amount=${S2_AMOUNT} status=paid"
  echo "S2_snapshot_id=${S2_ID}"
  echo "S2_commit_time=${S2_TIME}"
  echo "S3_action=DELETE (row absent)"
  echo "S3_snapshot_id=${S3_ID}"
  echo "S3_commit_time=${S3_TIME}"
  echo "current_state=absent (after S3)"
  echo "historical_S1=amount≈100 present"
  echo "historical_S2=amount≈200 present"
  echo "historical_S3=absent"
  echo "dedicated_table=yes (does not mutate ods_orders / G1–G7 counts)"
  echo "NOT claimed: EO-2PC / Exactly-Once E2E / G9–G10 / full reconcile / compaction / continuous CDC time travel"
  echo "PASS"
} >"$EVIDENCE_FILE"

echo "[G8] PASS time travel"
echo "[g8] evidence → ${EVIDENCE_FILE}"
