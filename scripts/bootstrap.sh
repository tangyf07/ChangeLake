#!/usr/bin/env bash
# Download Paimon + shaded Hadoop + Flink CDC MySQL SQL connector + MySQL JDBC.
# Optionally bring the Compose stack up.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

PAIMON_VERSION="${PAIMON_VERSION:-1.4.2}"
FLINK_MINOR="${FLINK_MINOR:-1.18}"
# Pre-bundled Hadoop jar required for filesystem warehouse outside a Hadoop cluster.
HADOOP_UBER_VERSION="${HADOOP_UBER_VERSION:-2.8.3-10.0}"
# Flink CDC SQL connector — tested matrix: Flink 1.18.x + mysql-cdc 3.1.1
# Docs: https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/mysql-cdc/
FLINK_CDC_VERSION="${FLINK_CDC_VERSION:-3.1.1}"
# MySQL JDBC (GPL) is NOT bundled inside flink-sql-connector-mysql-cdc; download separately.
MYSQL_JDBC_VERSION="${MYSQL_JDBC_VERSION:-8.0.33}"

LIB_DIR="$ROOT/flink/lib"
mkdir -p "$LIB_DIR"

PAIMON_JAR="paimon-flink-${FLINK_MINOR}-${PAIMON_VERSION}.jar"
PAIMON_URL="https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-${FLINK_MINOR}/${PAIMON_VERSION}/${PAIMON_JAR}"

HADOOP_JAR="flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar"
HADOOP_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/${HADOOP_UBER_VERSION}/${HADOOP_JAR}"

CDC_JAR="flink-sql-connector-mysql-cdc-${FLINK_CDC_VERSION}.jar"
CDC_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-mysql-cdc/${FLINK_CDC_VERSION}/${CDC_JAR}"

MYSQL_JDBC_JAR="mysql-connector-j-${MYSQL_JDBC_VERSION}.jar"
MYSQL_JDBC_URL="https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/${MYSQL_JDBC_VERSION}/${MYSQL_JDBC_JAR}"

download() {
  local url="$1"
  local dest="$2"
  if [[ -f "$dest" ]]; then
    echo "[bootstrap] exists: $(basename "$dest")"
    return 0
  fi
  echo "[bootstrap] downloading $(basename "$dest")"
  echo "            from $url"
  curl -fL --retry 3 --retry-delay 2 -o "${dest}.partial" "$url"
  mv "${dest}.partial" "$dest"
}

echo "[bootstrap] Paimon ${PAIMON_VERSION} for Flink ${FLINK_MINOR}"
echo "[bootstrap] Flink CDC mysql-cdc ${FLINK_CDC_VERSION} + MySQL JDBC ${MYSQL_JDBC_VERSION}"
echo "[bootstrap] Paimon docs: https://paimon.apache.org/docs/1.4/flink/quick-start/"
echo "[bootstrap] CDC docs:    https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/mysql-cdc/"

download "$PAIMON_URL" "$LIB_DIR/$PAIMON_JAR"
download "$HADOOP_URL" "$LIB_DIR/$HADOOP_JAR"
download "$CDC_URL" "$LIB_DIR/$CDC_JAR"
download "$MYSQL_JDBC_URL" "$LIB_DIR/$MYSQL_JDBC_JAR"

ls -lh "$LIB_DIR"/*.jar

if [[ "${1:-}" == "--jars-only" ]]; then
  echo "[bootstrap] jars-only done"
  exit 0
fi

if [[ ! -f .env ]]; then
  cp .env.example .env
  echo "[bootstrap] created .env from .env.example (demo credentials)"
fi

docker compose up -d
bash "$ROOT/scripts/wait_services.sh"
echo "[bootstrap] done"
