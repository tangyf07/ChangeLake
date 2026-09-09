#!/usr/bin/env bash
# Ensure MinIO is healthy and bucket exists (idempotent).
# Prefer compose minio-init service; this script is a manual/fallback helper.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -f .env ]]; then
  # shellcheck disable=SC1091
  set -a
  source .env
  set +a
fi

MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin}"
MINIO_BUCKET="${MINIO_BUCKET:-changelake}"
MINIO_API_PORT="${MINIO_API_PORT:-9000}"
TIMEOUT="${WAIT_TIMEOUT:-120}"

echo "[minio_init] waiting for MinIO live on :${MINIO_API_PORT}"
deadline=$((SECONDS + TIMEOUT))
while (( SECONDS < deadline )); do
  if curl -sf "http://localhost:${MINIO_API_PORT}/minio/health/live" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
if ! curl -sf "http://localhost:${MINIO_API_PORT}/minio/health/live" >/dev/null 2>&1; then
  echo "[minio_init] TIMEOUT: MinIO not live" >&2
  exit 1
fi

echo "[minio_init] creating bucket '${MINIO_BUCKET}' via compose minio-init (or mc)"
if docker compose config --services 2>/dev/null | grep -qx minio-init; then
  docker compose run --rm minio-init
else
  docker run --rm --network changelake_net \
    -e MINIO_ROOT_USER="$MINIO_ROOT_USER" \
    -e MINIO_ROOT_PASSWORD="$MINIO_ROOT_PASSWORD" \
    -e MINIO_BUCKET="$MINIO_BUCKET" \
    minio/mc:RELEASE.2025-07-21T05-28-08Z \
    /bin/sh -c '
      set -e
      mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
      mc mb --ignore-existing "local/$MINIO_BUCKET"
      mc ls local/
    '
fi

echo "[minio_init] done"
