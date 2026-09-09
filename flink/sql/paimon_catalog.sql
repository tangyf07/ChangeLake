-- ChangeLake: Paimon catalog on MinIO (S3-compatible)
-- Docs: https://paimon.apache.org/docs/1.4/maintenance/filesystems/
-- Requires: paimon-flink-1.18-1.4.2.jar + paimon-s3-1.4.2.jar
--           (+ flink-shaded-hadoop uber) in Flink lib/
-- Demo-only keys below; keep in sync with .env.example / scripts/common.sh.

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = 's3://changelake/warehouse',
  's3.endpoint' = 'http://minio:9000',
  's3.access-key' = 'minioadmin',
  's3.secret-key' = 'minioadmin',
  's3.path.style.access' = 'true'
);

USE CATALOG paimon;

CREATE DATABASE IF NOT EXISTS ods;
CREATE DATABASE IF NOT EXISTS dwd;
CREATE DATABASE IF NOT EXISTS ads;
