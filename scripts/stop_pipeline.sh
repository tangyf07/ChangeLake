#!/usr/bin/env bash
# Stop ChangeLake ODS CDC Flink job(s).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/scripts/common.sh"

echo "[pipeline] stopping jobs matching '${PIPELINE_JOB_NAME}'"
cancel_flink_jobs "$PIPELINE_JOB_NAME"
echo "[pipeline] stop requested"
