#!/usr/bin/env bash
# Storage smoke: Flink → Paimon → MinIO write/read.
# Run AFTER: jars downloaded, compose up, wait_services (MinIO healthy + bucket).
# Must PASS before Golden Path G1–G4.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck disable=SC1091
source "$ROOT/scripts/common.sh"

echo "[smoke_storage] ensuring jars (incl. paimon-s3-${PAIMON_VERSION:-1.4.2}.jar)"
need_jars=0
compgen -G "$ROOT/flink/lib/paimon-flink-*.jar" >/dev/null || need_jars=1
compgen -G "$ROOT/flink/lib/paimon-s3-*.jar" >/dev/null || need_jars=1
compgen -G "$ROOT/flink/lib/flink-shaded-hadoop-2-uber-*.jar" >/dev/null || need_jars=1
if (( need_jars == 1 )); then
  bash "$ROOT/scripts/bootstrap.sh" --jars-only
fi

# Restart JM/TM if paimon-s3 not yet in container lib
if ! docker compose exec -T jobmanager bash -lc 'compgen -G "/opt/flink/lib/paimon-s3-*.jar" >/dev/null'; then
  echo "[smoke_storage] paimon-s3 jar not in JM lib — restarting jobmanager/taskmanager"
  docker compose restart jobmanager taskmanager
  bash "$ROOT/scripts/wait_services.sh"
fi

MINIO_API_PORT="${MINIO_API_PORT:-9000}"
if ! curl -sf "http://localhost:${MINIO_API_PORT}/minio/health/live" >/dev/null; then
  echo "[smoke_storage] FAIL: MinIO not healthy on :${MINIO_API_PORT}" >&2
  exit 2
fi
echo "[smoke_storage] MinIO live; warehouse=${PAIMON_WAREHOUSE} endpoint=${MINIO_ENDPOINT}"

echo "[smoke_storage] Flink → Paimon CREATE/INSERT/SELECT on MinIO"
out="$(paimon_sql <<'SQL'
CREATE DATABASE IF NOT EXISTS smoke;
DROP TABLE IF EXISTS smoke.storage_probe;
CREATE TABLE smoke.storage_probe (
  id BIGINT,
  note STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'bucket' = '1'
);
INSERT INTO smoke.storage_probe VALUES (1, 'minio-ok');
SELECT id, note FROM smoke.storage_probe WHERE id = 1;
SQL
)"

echo "$out"

# D1: require a SELECT result row (tableau), not merely the INSERT VALUES text
if ! echo "$out" | grep -E '\|\s*1\s*\|\s*minio-ok\s*\|'; then
  echo "[smoke_storage] FAIL: did not read back probe row (id=1, note=minio-ok) from Paimon/MinIO" >&2
  echo "$out" >&2
  exit 2
fi

# Optional: confirm object prefix exists via mc (best-effort)
if docker compose ps minio >/dev/null 2>&1; then
  if docker run --rm --network changelake_net \
    -e MINIO_ROOT_USER="$MINIO_ROOT_USER" \
    -e MINIO_ROOT_PASSWORD="$MINIO_ROOT_PASSWORD" \
    minio/mc:RELEASE.2025-07-21T05-28-08Z \
    /bin/sh -c '
      mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
      mc ls --recursive "local/changelake/warehouse/" 2>/dev/null | head -n 20
    ' 2>/dev/null | tee /tmp/changelake_smoke_mc.txt | grep -q .; then
    echo "[smoke_storage] MinIO objects under warehouse/ (sample):"
    head -n 10 /tmp/changelake_smoke_mc.txt || true
  else
    echo "[smoke_storage] note: mc list empty/unavailable (Flink readback already passed)"
  fi
fi

echo "[smoke_storage] PASS Flink → Paimon → MinIO write/read"
