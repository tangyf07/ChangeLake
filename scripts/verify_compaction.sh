#!/usr/bin/env bash
# Alias: Phase 9 / G10 verification entrypoint → scripts/compaction.sh
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec bash "$ROOT/scripts/compaction.sh" "$@"
