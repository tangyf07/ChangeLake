#!/usr/bin/env bash
# Alias: Phase 7 / G8 verification entrypoint → scripts/time_travel.sh
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
exec bash "$ROOT/scripts/time_travel.sh" "$@"
