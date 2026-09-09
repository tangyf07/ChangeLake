#!/usr/bin/env bash
# ChangeLake Phase 9 / G10: Paimon Compaction demo.
#
# Dedicated table ods.ods_compact_demo (does NOT mutate golden-path ODS counts):
#   1) CREATE with write-only=true so writers skip compaction
#   2) Many small-batch writes (default 30×100 = 3k rows via datagen, chunked sql-client → many L0 files)
#   3) Record before: snapshot count, file count, total file size, query latency, fingerprint
#   4) CALL sys.compact (Flink 1.18 positional args; compact_strategy=full in batch)
#   5) Record after: file count, size, latency, fingerprint
#   6) HARD assert: fingerprint(before) == fingerprint(after)
#   7) SOFT expect: file_count_after < file_count_before (document if noisy; do NOT fail)
#
# Success: [G10] PASS compaction
# Honest: demo compaction only — NOT production sizing SLA; NOT EO-2PC; NOT latency SLA.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/scripts/common.sh"

EVIDENCE_DIR="$ROOT/docs/evidence"
EVIDENCE_FILE="$EVIDENCE_DIR/g10_compaction.txt"
mkdir -p "$EVIDENCE_DIR"

TABLE="ods.ods_compact_demo"
TABLE_BARE="ods_compact_demo"
# Batches × rows_per_batch — keep thousands of rows + multiple commits for file_count>1.
# Default 30×100=3000 via datagen (avoid multi-row VALUES → N Calc ops / network buffer exhaustion).
COMPACT_BATCHES="${COMPACT_BATCHES:-30}"
COMPACT_ROWS_PER_BATCH="${COMPACT_ROWS_PER_BATCH:-100}"
# How many INSERT statements per sql-client -f session (1–5). Prefer small; wait each chunk.
COMPACT_INSERTS_PER_SESSION="${COMPACT_INSERTS_PER_SESSION:-1}"
# After writes, poll COUNT(*) until expected (or timeout).
COMPACT_ROW_WAIT_TIMEOUT="${COMPACT_ROW_WAIT_TIMEOUT:-180}"
# Latency query: ordered dump used for fingerprint (same SQL before/after)
LATENCY_REPEAT="${LATENCY_REPEAT:-1}"

fail() {
  echo "[G10] FAIL $*"
  {
    echo "G10 Compaction / Phase 9"
    echo "table=${TABLE}"
    echo "FAIL: $*"
    echo "NOT claimed: production sizing SLA / EO-2PC / Exactly-Once E2E / latency % drop guarantee"
  } >"$EVIDENCE_FILE"
  exit 2
}

# Parse two aggregates from tableau: first numeric = count, second = sum (or size).
parse_two_scalars() {
  python3 -c '
import re, sys
text = sys.stdin.read()
vals = []
for line in text.splitlines():
    if "|" not in line:
        continue
    s = line.strip()
    if set(s) <= set("+-| "):
        continue
    low = s.lower()
    if "row" in low and "set" in low:
        continue
    cells = [x.strip() for x in line.split("|") if x.strip() != ""]
    if not cells:
        continue
    if any(h in " ".join(cells).lower() for h in ("file_count", "total_size", "snapshot", "count(", "sum(")):
        # header-ish
        if not all(re.fullmatch(r"-?\d+(\.\d+)?", c) for c in cells):
            continue
    nums = [c for c in cells if re.fullmatch(r"-?\d+(\.\d+)?", c)]
    if len(nums) >= 2:
        print(f"{nums[0]} {nums[1]}")
        sys.exit(0)
    if len(nums) == 1 and not vals:
        vals.append(nums[0])
if len(vals) >= 1:
    # fallback single
    print(f"{vals[0]} 0")
    sys.exit(0)
sys.stderr.write("parse_two_scalars failed:\n" + text + "\n")
sys.exit(1)
'
}

snapshot_count() {
  local out
  out="$(paimon_sql <<SQL
SELECT COUNT(*) FROM ods.\`${TABLE_BARE}\$snapshots\`;
SQL
)" || return 1
  echo "$out" | extract_plain_scalar
}

file_stats() {
  # Prints: file_count total_size_bytes
  local out
  out="$(paimon_sql <<SQL
SELECT COUNT(*) AS file_count, CAST(COALESCE(SUM(file_size_in_bytes), 0) AS BIGINT) AS total_size
FROM ods.\`${TABLE_BARE}\$files\`;
SQL
)" || return 1
  echo "$out" | parse_two_scalars
}

# Ordered row dump for fingerprint (content-only).
dump_rows() {
  paimon_sql <<SQL
SELECT id, CAST(amount AS STRING) AS amount, status
FROM ${TABLE}
ORDER BY id;
SQL
}

fingerprint_from_dump() {
  local dump="$1"
  python3 -c '
import hashlib, re, sys
text = sys.argv[1]
lines = []
for line in text.splitlines():
    if "|" not in line:
        continue
    s = line.strip()
    if set(s) <= set("+-| "):
        continue
    low = s.lower()
    if "row" in low and "set" in low:
        continue
    cells = [x.strip() for x in line.split("|")]
    if cells and cells[0] == "":
        cells = cells[1:]
    if cells and cells[-1] == "":
        cells = cells[:-1]
    cells = [c.strip() for c in cells]
    if not cells:
        continue
    joined = " ".join(cells).lower()
    if "id" in joined and "amount" in joined and not re.fullmatch(r"-?\d+(\.\d+)?", cells[0]):
        continue
    # expect id, amount, status
    if len(cells) < 2:
        continue
    if not re.fullmatch(r"-?\d+", cells[0]):
        continue
    amt = cells[1]
    status = cells[2] if len(cells) > 2 else ""
    lines.append(f"{cells[0]}\t{amt}\t{status}")
lines.sort()
body = "\n".join(lines) + ("\n" if lines else "")
print(hashlib.sha256(body.encode("utf-8")).hexdigest())
print(len(lines), file=sys.stderr)
' "$dump" 2>/tmp/changelake_g10_fp_rows.txt
}

# Timed dump: prints milliseconds to stdout line "LATENCY_MS=N", dump to file arg.
timed_dump_to() {
  local out_file="$1"
  local start_ns end_ns ms
  start_ns="$(date +%s%N)"
  dump_rows >"$out_file" || return 1
  end_ns="$(date +%s%N)"
  ms=$(( (end_ns - start_ns) / 1000000 ))
  echo "LATENCY_MS=${ms}"
}

# Emit one batch write via Flink datagen (NOT multi-row VALUES).
# Large VALUES expands to Values→Calc[2]…Calc[N] and exhausts network buffers
# ("required 512, but only 0 available") even with a bigger buffer pool.
batch_insert_sql() {
  local start_id="$1"
  local n="$2"
  local end_id=$((start_id + n - 1))
  local gen="_g10_gen_${start_id}_${end_id}"
  cat <<SQL
DROP TEMPORARY TABLE IF EXISTS ${gen};
CREATE TEMPORARY TABLE ${gen} (
  id BIGINT
) WITH (
  'connector' = 'datagen',
  'number-of-rows' = '${n}',
  'fields.id.kind' = 'sequence',
  'fields.id.start' = '${start_id}',
  'fields.id.end' = '${end_id}'
);
INSERT INTO ${TABLE}
SELECT
  id,
  CAST(id AS DECIMAL(12, 2)) / 100,
  'open',
  TIMESTAMP '2026-09-09 12:00:00'
FROM ${gen};
SQL
}

# Cancel leftover G10 insert jobs (RESTARTING/RUNNING chew network buffers).
# Also snapshot pre-existing FAILED jids so wait_insert ignores historical noise.
G10_IGNORE_FAILED_JIDS="${G10_IGNORE_FAILED_JIDS:-/tmp/changelake_g10_ignore_failed_jids.txt}"

cancel_compact_insert_jobs() {
  : >"$G10_IGNORE_FAILED_JIDS"
  python3 -c '
import json, urllib.request, sys
ui = sys.argv[1].rstrip("/")
ignore_path = sys.argv[2]
try:
    data = json.load(urllib.request.urlopen(ui + "/jobs/overview", timeout=8))
except Exception as e:
    print(f"[g10] WARN: cannot list jobs to cancel: {e}", file=sys.stderr)
    open(ignore_path, "w").close()
    sys.exit(0)
cancel_states = {"RUNNING", "RESTARTING", "CREATED", "INITIALIZING", "FAILING", "CANCELLING"}
n = 0
ignored = []
for j in data.get("jobs", []):
    name = (j.get("name") or "").lower()
    state = (j.get("state") or "").upper()
    jid = j.get("jid") or j.get("id")
    if not jid:
        continue
    if not (
        "ods_compact" in name
        or "compact_demo" in name
        or ("insert" in name and "compact" in name)
    ):
        continue
    if state == "FAILED":
        ignored.append(jid)
        print(f"[g10] ignore pre-existing FAILED insert job {jid}")
        continue
    if state not in cancel_states:
        continue
    req = urllib.request.Request(ui + f"/jobs/{jid}?mode=cancel", method="PATCH")
    try:
        urllib.request.urlopen(req, timeout=8).read()
        print(f"[g10] cancelled insert job {jid} state={state} name={name}")
        n += 1
    except Exception as e:
        print(f"[g10] WARN: cancel {jid} failed: {e}", file=sys.stderr)
with open(ignore_path, "w", encoding="utf-8") as f:
    f.write("\n".join(ignored) + ("\n" if ignored else ""))
print(f"[g10] cancelled {n} active compact INSERT job(s); ignoring {len(ignored)} old FAILED")
' "$(flink_ui)" "$G10_IGNORE_FAILED_JIDS" || true
}

# After a batch INSERT session: fail hard on FAILED; wait until no active INSERT jobs.
# Root cause previously: wait treated FAILED as idle while FixedDelayRestart chewed network buffers
# ("Insufficient number of network buffers: required 512, but only 0 available").
wait_insert_jobs_finished() {
  local timeout="${1:-90}"
  local since_ms="${2:-0}"
  local deadline=$((SECONDS + timeout))
  local report
  while (( SECONDS < deadline )); do
    report="$(python3 -c '
import json, urllib.request, sys
ui = sys.argv[1]
ignore_path = sys.argv[2]
since_ms = int(sys.argv[3])
ignore = set()
try:
    with open(ignore_path, encoding="utf-8") as f:
        ignore = {ln.strip() for ln in f if ln.strip()}
except FileNotFoundError:
    pass
try:
    data = json.load(urllib.request.urlopen(ui + "/jobs/overview", timeout=8))
except Exception as e:
    print(f"ERR:{e}")
    sys.exit(0)
active_states = {"RUNNING", "RESTARTING", "CREATED", "INITIALIZING", "CANCELLING", "FAILING"}
active = 0
failed = []
for j in data.get("jobs", []):
    name = (j.get("name") or "")
    low = name.lower()
    state = (j.get("state") or "").upper()
    jid = j.get("jid") or j.get("id") or "?"
    start = int(j.get("start-time") or 0)
    if "compact" not in low and "ods_compact" not in low:
        continue
    if state in active_states:
        active += 1
    elif state == "FAILED" and jid not in ignore and (since_ms <= 0 or start >= since_ms - 2000):
        failed.append(f"{jid}:{state}:{name}")
print(f"OK:{active}:{len(failed)}")
for f in failed[:5]:
    print(f"FAILJOB:{f}")
' "$(flink_ui)" "${G10_IGNORE_FAILED_JIDS:-/tmp/changelake_g10_ignore_failed_jids.txt}" "$since_ms" 2>/dev/null || echo "ERR:python")"
    if [[ "$report" == ERR:* ]] || [[ "$(echo "$report" | head -1)" == ERR:* ]]; then
      echo "[g10] WARN: Flink jobs overview unavailable; relying on sql-client -f wait"
      return 0
    fi
    local head active_n fail_n
    head="$(echo "$report" | head -1)"
    active_n="${head#OK:}"; active_n="${active_n%%:*}"
    fail_n="${head##*:}"
    if [[ "$fail_n" =~ ^[0-9]+$ ]] && (( fail_n > 0 )); then
      echo "$report" | grep '^FAILJOB:' | head -5 || true
      # Print root exception for first FAILJOB
      fj="$(echo "$report" | grep '^FAILJOB:' | head -1 | sed 's/^FAILJOB://')"
      jid="${fj%%:*}"
      if [[ -n "$jid" && "$jid" != "?" ]]; then
        python3 -c '
import json,urllib.request,sys
ui=sys.argv[1].rstrip("/"); jid=sys.argv[2]
try:
  d=json.load(urllib.request.urlopen(ui+f"/jobs/{jid}/exceptions",timeout=10))
  print(d.get("root-exception","")[:2500])
except Exception as e:
  print(f"(no exception body: {e})")
' "$(flink_ui)" "$jid" || true
      fi
      fail "INSERT job FAILED. See FAILJOB + root-exception above"
    fi
    if [[ "$active_n" =~ ^[0-9]+$ ]] && (( active_n == 0 )); then
      return 0
    fi
    echo "[g10] waiting for INSERT job(s) FINISHED (active≈${active_n})"
    sleep 2
  done
  echo "[g10] WARN: timed out waiting for INSERT jobs idle (continuing; COUNT(*) gate follows)"
  return 0
}

# Poll COUNT(*) until it reaches expected (or timeout). Prints final count on stdout.
wait_row_count() {
  local expected="$1"
  local timeout="${2:-$COMPACT_ROW_WAIT_TIMEOUT}"
  local deadline=$((SECONDS + timeout))
  local out cnt="0"
  while (( SECONDS < deadline )); do
    out="$(paimon_sql <<SQL
SELECT COUNT(*) FROM ${TABLE};
SQL
)" || out=""
    if cnt="$(echo "$out" | extract_plain_scalar 2>/dev/null)"; then
      if [[ "$cnt" == "$expected" ]]; then
        echo "$cnt"
        return 0
      fi
      echo "[g10] waiting for row_count=${expected} (now=${cnt})" >&2
    else
      echo "[g10] waiting for row_count=${expected} (COUNT parse pending)" >&2
      cnt="0"
    fi
    sleep 3
  done
  echo "$cnt"
  return 1
}

echo "=================================================="
echo "[G10] Compaction / Phase 9"
echo "=================================================="
echo "[g10] table=${TABLE} (dedicated; does not touch ods_orders / G1–G9 counts)"
echo "[g10] plan: write-only chunked small batches → wait COUNT(*) → stats/fingerprint → CALL sys.compact(full) → re-check"
echo "[g10] batches=${COMPACT_BATCHES} rows_per_batch=${COMPACT_ROWS_PER_BATCH} (~$((COMPACT_BATCHES * COMPACT_ROWS_PER_BATCH)) rows)"
echo "[g10] compact: Flink 1.18 positional CALL sys.compact(..., compact_strategy='full') in batch mode"
echo "[g10] hard gate: query fingerprint identical before vs after"
echo "[g10] soft expect: file count reduced (document if not; do not hard-fail)"

# ---- Create / reset dedicated demo table ----
echo "[g10] creating ${TABLE} with write-only=true (writers skip compaction)"
paimon_sql <<'SQL' >/dev/null || fail "CREATE/DROP ods.ods_compact_demo failed"
CREATE DATABASE IF NOT EXISTS ods;
DROP TABLE IF EXISTS ods.ods_compact_demo;
CREATE TABLE ods.ods_compact_demo (
  id BIGINT,
  amount DECIMAL(12, 2),
  status STRING,
  updated_at TIMESTAMP(0),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'bucket' = '1',
  'write-only' = 'true',
  'sink.parallelism' = '1',
  'snapshot.num-retained.min' = '10',
  'snapshot.num-retained.max' = '200',
  'file.format' = 'parquet'
);
SQL

# ---- Many small-batch writes (chunked; never one giant multi-INSERT blob) ----
# Each chunk: build a small SQL file → paimon_sql copies it into jobmanager and runs sql-client -f.
# Default COMPACT_INSERTS_PER_SESSION=1 so each INSERT is its own session + commit.
EXPECTED_ROWS=$((COMPACT_BATCHES * COMPACT_ROWS_PER_BATCH))
if ! [[ "$COMPACT_INSERTS_PER_SESSION" =~ ^[1-9][0-9]*$ ]] || (( COMPACT_INSERTS_PER_SESSION < 1 || COMPACT_INSERTS_PER_SESSION > 5 )); then
  fail "COMPACT_INSERTS_PER_SESSION must be 1..5 (got ${COMPACT_INSERTS_PER_SESSION})"
fi
echo "[g10] writing ${COMPACT_BATCHES} batches × ${COMPACT_ROWS_PER_BATCH} rows (=${EXPECTED_ROWS}; write-only → many small files)"
echo "[g10] write path: datagen→INSERT SELECT per chunk (sql-client -f; no multi-row VALUES)"
echo "[g10] session knobs: parallelism.default=1"

# Clear leftover FAILED/RESTARTING compact inserts from prior runs (frees TM network buffers).
cancel_compact_insert_jobs

next_id=1
batch_num=0
while (( batch_num < COMPACT_BATCHES )); do
  chunk_n=0
  WRITE_SQL="$(mktemp)"
  {
    echo "SET 'parallelism.default' = '1';"
    while (( chunk_n < COMPACT_INSERTS_PER_SESSION && batch_num < COMPACT_BATCHES )); do
      batch_insert_sql "$next_id" "$COMPACT_ROWS_PER_BATCH"
      echo
      next_id=$((next_id + COMPACT_ROWS_PER_BATCH))
      batch_num=$((batch_num + 1))
      chunk_n=$((chunk_n + 1))
    done
  } >"$WRITE_SQL"
  echo "[g10] submitting chunk batches $((batch_num - chunk_n + 1))–${batch_num}/${COMPACT_BATCHES} (${chunk_n} INSERT(s), ids up to $((next_id - 1)))"
  CHUNK_SUBMIT_MS="$(python3 -c 'import time; print(int(time.time()*1000))')"
  if ! paimon_sql <"$WRITE_SQL" >/dev/null; then
    rm -f "$WRITE_SQL"
    fail "chunked INSERT failed at batch≈${batch_num}/${COMPACT_BATCHES}"
  fi
  rm -f "$WRITE_SQL"
  wait_insert_jobs_finished 90 "$CHUNK_SUBMIT_MS"
done

# Verify row count with timeout (eventual visibility after last commit)
echo "[g10] waiting until COUNT(*)=${EXPECTED_ROWS} (timeout=${COMPACT_ROW_WAIT_TIMEOUT}s)"
if ! ROW_CNT="$(wait_row_count "$EXPECTED_ROWS" "$COMPACT_ROW_WAIT_TIMEOUT")"; then
  fail "expected ${EXPECTED_ROWS} rows after writes, got ${ROW_CNT:-0}"
fi
echo "[g10] row_count=${ROW_CNT}"

# ---- BEFORE stats ----
echo "[g10] collecting BEFORE compaction stats"
BEFORE_SNAP="$(snapshot_count)" || fail "snapshot count (before) failed"
BEFORE_FILES_RAW="$(file_stats)" || fail "file stats (before) failed"
BEFORE_FILE_COUNT="$(echo "$BEFORE_FILES_RAW" | awk '{print $1}')"
BEFORE_FILE_SIZE="$(echo "$BEFORE_FILES_RAW" | awk '{print $2}')"

BEFORE_DUMP="$(mktemp)"
BEFORE_LAT_LINE="$(timed_dump_to "$BEFORE_DUMP")" || fail "before dump/latency failed"
BEFORE_LATENCY_MS="${BEFORE_LAT_LINE#LATENCY_MS=}"
BEFORE_FP="$(fingerprint_from_dump "$(cat "$BEFORE_DUMP")")" || fail "before fingerprint failed"
BEFORE_FP_ROWS="$(cat /tmp/changelake_g10_fp_rows.txt 2>/dev/null || echo '?')"

echo "[g10] BEFORE snapshot_count=${BEFORE_SNAP} file_count=${BEFORE_FILE_COUNT} total_file_size_bytes=${BEFORE_FILE_SIZE}"
echo "[g10] BEFORE query_latency_ms=${BEFORE_LATENCY_MS} fingerprint=${BEFORE_FP} fp_rows=${BEFORE_FP_ROWS}"

if [[ "$BEFORE_FP_ROWS" != "$EXPECTED_ROWS" ]]; then
  fail "before fingerprint row count ${BEFORE_FP_ROWS} != expected ${EXPECTED_ROWS}"
fi
if ! [[ "$BEFORE_FILE_COUNT" =~ ^[0-9]+$ ]] || (( BEFORE_FILE_COUNT < 2 )); then
  echo "[g10] WARN: before file_count=${BEFORE_FILE_COUNT} is low; write-only may not have produced many files (continuing)"
fi

# ---- Compact ----
echo "[g10] invoking CALL sys.compact (Flink 1.18 positional; strategy=full)"
# Flink 1.18: positional only. Args:
#   table, partitions, order_strategy, order_by, options, where, partition_idle_time, compact_strategy
# Use '' placeholders; full strategy selects all files for merge (batch mode).
COMPACT_OUT="$(paimon_sql <<'SQL'
-- Dedicated compaction (batch). write-only writers skipped compaction during inserts.
CALL sys.compact('ods.ods_compact_demo', '', '', '', 'sink.parallelism=1', '', '', 'full');
SQL
)" || fail "CALL sys.compact failed"
echo "$COMPACT_OUT" | tail -n 30
echo "[g10] compact CALL finished"

# Brief settle (manifest commit)
sleep 2

# ---- AFTER stats ----
echo "[g10] collecting AFTER compaction stats"
AFTER_SNAP="$(snapshot_count)" || AFTER_SNAP="?"
AFTER_FILES_RAW="$(file_stats)" || fail "file stats (after) failed"
AFTER_FILE_COUNT="$(echo "$AFTER_FILES_RAW" | awk '{print $1}')"
AFTER_FILE_SIZE="$(echo "$AFTER_FILES_RAW" | awk '{print $2}')"

AFTER_DUMP="$(mktemp)"
AFTER_LAT_LINE="$(timed_dump_to "$AFTER_DUMP")" || fail "after dump/latency failed"
AFTER_LATENCY_MS="${AFTER_LAT_LINE#LATENCY_MS=}"
AFTER_FP="$(fingerprint_from_dump "$(cat "$AFTER_DUMP")")" || fail "after fingerprint failed"
AFTER_FP_ROWS="$(cat /tmp/changelake_g10_fp_rows.txt 2>/dev/null || echo '?')"

echo "[g10] AFTER  snapshot_count=${AFTER_SNAP} file_count=${AFTER_FILE_COUNT} total_file_size_bytes=${AFTER_FILE_SIZE}"
echo "[g10] AFTER  query_latency_ms=${AFTER_LATENCY_MS} fingerprint=${AFTER_FP} fp_rows=${AFTER_FP_ROWS}"

# ---- HARD assert: fingerprint ----
if [[ "$BEFORE_FP" != "$AFTER_FP" ]]; then
  fail "fingerprint mismatch before=${BEFORE_FP} after=${AFTER_FP} (data must be identical)"
fi
echo "[g10] HARD PASS: fingerprint identical (${BEFORE_FP})"

# ---- SOFT: file count reduction ----
FILE_NOTE="file_count ${BEFORE_FILE_COUNT} → ${AFTER_FILE_COUNT}"
SOFT_FILE="unknown"
if [[ "$BEFORE_FILE_COUNT" =~ ^[0-9]+$ && "$AFTER_FILE_COUNT" =~ ^[0-9]+$ ]]; then
  if (( AFTER_FILE_COUNT < BEFORE_FILE_COUNT )); then
    SOFT_FILE="reduced"
    FILE_NOTE="file_count reduced ${BEFORE_FILE_COUNT} → ${AFTER_FILE_COUNT} (soft PASS)"
  elif (( AFTER_FILE_COUNT == BEFORE_FILE_COUNT )); then
    SOFT_FILE="unchanged"
    FILE_NOTE="file_count unchanged ${BEFORE_FILE_COUNT} (soft noise; not a hard fail)"
  else
    SOFT_FILE="increased"
    FILE_NOTE="file_count increased ${BEFORE_FILE_COUNT} → ${AFTER_FILE_COUNT} (soft noise; not a hard fail)"
  fi
fi
echo "[g10] SOFT: ${FILE_NOTE}"

LAT_NOTE="latency_ms ${BEFORE_LATENCY_MS} → ${AFTER_LATENCY_MS} (informational only; local small data is noisy — NOT a hard gate)"
echo "[g10] ${LAT_NOTE}"

rm -f "$BEFORE_DUMP" "$AFTER_DUMP"

# ---- Evidence ----
{
  echo "G10 Compaction / Phase 9"
  echo "table=${TABLE}"
  echo "method=CALL sys.compact('ods.ods_compact_demo', '', '', '', 'sink.parallelism=1', '', '', 'full')"
  echo "flink=1.18.1 positional procedure args; paimon=1.4.2; compact_strategy=full; runtime-mode=batch"
  echo "write_mode=write-only=true; datagen→INSERT SELECT (${COMPACT_INSERTS_PER_SESSION}/session; scale=${COMPACT_BATCHES}x${COMPACT_ROWS_PER_BATCH})"
  echo "batches=${COMPACT_BATCHES} rows_per_batch=${COMPACT_ROWS_PER_BATCH} row_count=${ROW_CNT}"
  echo "BEFORE snapshot_count=${BEFORE_SNAP} file_count=${BEFORE_FILE_COUNT} total_file_size_bytes=${BEFORE_FILE_SIZE} query_latency_ms=${BEFORE_LATENCY_MS}"
  echo "AFTER  snapshot_count=${AFTER_SNAP} file_count=${AFTER_FILE_COUNT} total_file_size_bytes=${AFTER_FILE_SIZE} query_latency_ms=${AFTER_LATENCY_MS}"
  echo "fingerprint_before=${BEFORE_FP}"
  echo "fingerprint_after=${AFTER_FP}"
  echo "fingerprint_match=yes"
  echo "soft_file_count=${SOFT_FILE} (${FILE_NOTE})"
  echo "latency_note=${LAT_NOTE}"
  echo "dedicated_table=yes (does not mutate ods_orders / G1–G9 golden-path counts)"
  echo "NOT claimed: production sizing SLA / EO-2PC / Exactly-Once E2E / latency % drop / continuous CDC compaction"
  echo "PASS"
} >"$EVIDENCE_FILE"

echo "[G10] PASS compaction"
echo "[g10] evidence → ${EVIDENCE_FILE}"
exit 0
