#!/usr/bin/env bash
# Phase 4 / G6: checkpoint → kill TaskManager → restore → Paimon == MySQL.
#
# Proven (scripted): Flink checkpoint on named volume + fixed-delay restart after
# docker kill of changelake-taskmanager; CDC catch-up; ODS current-state match.
# NOT claimed: Exactly-Once E2E / EO-2PC, HA multi-JM, checkpoint-on-S3.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/scripts/common.sh"

EVIDENCE_DIR="$ROOT/docs/evidence"
mkdir -p "$EVIDENCE_DIR"
EVIDENCE_FILE="$EVIDENCE_DIR/g6_failure_recovery.txt"

fail_g6() {
  echo "[G6] FAIL failure recovery"
  printf '%s\n' "$@"
  {
    echo "G6 Failure Recovery"
    echo "FAIL"
    printf '%s\n' "$@"
  } >"$EVIDENCE_FILE"
  exit 2
}

wait_until() {
  local timeout="$1"
  local desc="$2"
  shift 2
  local deadline=$((SECONDS + timeout))
  echo "[g6] wait: ${desc} (timeout=${timeout}s)"
  while (( SECONDS < deadline )); do
    if "$@"; then
      return 0
    fi
    sleep "$CDC_POLL_INTERVAL"
  done
  echo "[g6] TIMEOUT: ${desc}" >&2
  return 1
}

echo "[g6] ChangeLake Phase 4 / G6 failure recovery"
echo "[g6] Flink UI: $(flink_ui)  job=${PIPELINE_JOB_NAME}"

if ! flink_job_is_running "$PIPELINE_JOB_NAME"; then
  fail_g6 "pipeline '${PIPELINE_JOB_NAME}' not RUNNING (start via start_pipeline.sh or demo G1–G5 first)"
fi

JID="$(flink_job_id "$PIPELINE_JOB_NAME")" || fail_g6 "could not resolve RUNNING job id"
echo "[g6] pre-kill job id=${JID}"

# 1) Wait for ≥1 completed checkpoint (proves checkpointing is live)
if ! wait_flink_checkpoint 1 "$CDC_WAIT_TIMEOUT" "$PIPELINE_JOB_NAME"; then
  fail_g6 "no completed checkpoint before fault injection" "job=${JID}"
fi
CP_BEFORE="$(flink_completed_checkpoint_count "$JID")"
echo "[g6] completed checkpoints before kill: ${CP_BEFORE}"

# 2) MySQL mutations while job RUNNING (may land after last checkpoint — restore + binlog catch-up)
echo "[g6] applying mysql/mutations/failure_recovery_pre.sql"
docker compose exec -T mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" < "$ROOT/mysql/mutations/failure_recovery_pre.sql"
mysql_scalar "SELECT CONCAT(order_id,':',status,':',amount) FROM changelake.orders WHERE order_id=3;"
mysql_scalar "SELECT CONCAT(order_id,':',status,':',amount) FROM changelake.orders WHERE order_id=900003;"

# Brief settle so JM sees activity; do NOT require another checkpoint (recovery must cover uncheckpointed CDC)
sleep 2

# 3) Kill TaskManager hard, then bring it back (shared /checkpoints named volume persists)
echo "[g6] docker kill changelake-taskmanager"
docker kill changelake-taskmanager >/dev/null || fail_g6 "docker kill changelake-taskmanager failed"
sleep 2
echo "[g6] docker compose up -d taskmanager"
docker compose up -d taskmanager
bash "$ROOT/scripts/wait_services.sh"

# 4) Wait until job is RUNNING again (failover / restart from checkpoint)
if ! wait_flink_job_running "$PIPELINE_JOB_NAME" "$CDC_WAIT_TIMEOUT"; then
  fail_g6 "job did not return to RUNNING after TaskManager restart"
fi
JID_AFTER="$(flink_job_id "$PIPELINE_JOB_NAME")" || fail_g6 "no RUNNING job id after restore"
echo "[g6] post-restore job id=${JID_AFTER} (may equal pre-kill jid if JM kept the job)"

# Optional: wait until checkpoints resume (proves writers healthy again)
if ! wait_flink_checkpoint 1 120 "$PIPELINE_JOB_NAME"; then
  echo "[g6] WARN: checkpoints not observed after restore within 120s (continuing to state compare)" >&2
fi

# 5) More UPDATEs after recovery
echo "[g6] applying mysql/mutations/failure_recovery_post.sql"
docker compose exec -T mysql mysql -u"$MYSQL_USER" -p"$MYSQL_PASSWORD" < "$ROOT/mysql/mutations/failure_recovery_post.sql"

check_order_match() {
  # check_order_match <order_id> <expected_status> <amount_regex>
  local oid="$1" estatus="$2" amount_re="$3"
  local mysql_status mysql_amount out
  mysql_status="$(mysql_scalar "SELECT status FROM changelake.orders WHERE order_id=${oid};")" || return 1
  mysql_amount="$(mysql_scalar "SELECT amount FROM changelake.orders WHERE order_id=${oid};")" || return 1
  [[ "$mysql_status" == "$estatus" ]] || return 1
  echo "$mysql_amount" | grep -Eq "$amount_re" || return 1
  out="$(ods_orders_query "$oid")" || return 1
  echo "$out" | grep -q "$oid" || return 1
  echo "$out" | grep -qw "$estatus" || return 1
  echo "$out" | grep -Eq "$amount_re" || return 1
  # single current-state row
  local cnt
  cnt="$(paimon_sql <<SQL
SELECT COUNT(*) FROM ods.ods_orders WHERE order_id = ${oid};
SQL
)"
  cnt="$(echo "$cnt" | extract_plain_scalar)" || return 1
  [[ "$cnt" == "1" ]]
}

check_g6_final() {
  check_order_match 3 shipped '302([.]22)?' || return 1
  check_order_match 900003 shipped '88([.]88)?' || return 1
}

if ! wait_until "$CDC_WAIT_TIMEOUT" "Paimon ods_orders matches MySQL for order_id=3,900003" check_g6_final; then
  out3="$(ods_orders_query 3 2>/dev/null || true)"
  out9="$(ods_orders_query 900003 2>/dev/null || true)"
  m3="$(mysql_scalar "SELECT CONCAT(order_id,':',status,':',amount) FROM changelake.orders WHERE order_id=3;" 2>/dev/null || true)"
  m9="$(mysql_scalar "SELECT CONCAT(order_id,':',status,':',amount) FROM changelake.orders WHERE order_id=900003;" 2>/dev/null || true)"
  fail_g6 "Paimon final state != MySQL after recovery" \
    "mysql order 3=${m3}" "lake order 3=${out3}" \
    "mysql order 900003=${m9}" "lake order 900003=${out9}"
fi

if ! flink_job_is_running "$PIPELINE_JOB_NAME"; then
  fail_g6 "pipeline not RUNNING after G6 assertions"
fi

OUT3="$(ods_orders_query 3)"
OUT9="$(ods_orders_query 900003)"
M3="$(mysql_scalar "SELECT CONCAT(order_id,':',status,':',amount) FROM changelake.orders WHERE order_id=3;")"
M9="$(mysql_scalar "SELECT CONCAT(order_id,':',status,':',amount) FROM changelake.orders WHERE order_id=900003;")"
echo "mysql 3=${M3}"
echo "lake  3=${OUT3}"
echo "mysql 900003=${M9}"
echo "lake  900003=${OUT9}"

{
  echo "G6 Failure Recovery"
  echo "checkpoint_dir=file:///checkpoints interval=10s backend=hashmap"
  echo "restart-strategy=fixed-delay attempts=10 delay=5s"
  echo "pre_kill_completed_checkpoints=${CP_BEFORE}"
  echo "pre_kill_job_id=${JID}"
  echo "post_restore_job_id=${JID_AFTER}"
  echo "fault=docker kill changelake-taskmanager → docker compose up -d taskmanager"
  echo "mysql order 3=${M3}"
  echo "lake order 3:"
  echo "$OUT3"
  echo "mysql order 900003=${M9}"
  echo "lake order 900003:"
  echo "$OUT9"
  echo "NOT claimed: EO-2PC / Exactly-Once E2E / multi-JM HA / checkpoint-on-S3"
  echo "PASS"
} >"$EVIDENCE_FILE"

echo "[G6] PASS failure recovery"
exit 0
