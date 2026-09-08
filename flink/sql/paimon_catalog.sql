-- ChangeLake Phase 1: Paimon filesystem catalog stub
-- Docs: https://paimon.apache.org/docs/1.4/flink/quick-start/
-- Requires: paimon-flink-1.18-1.4.2.jar (+ flink-shaded-hadoop uber) in Flink lib/
-- Warehouse path matches docker-compose volume mount.

CREATE CATALOG IF NOT EXISTS paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'file:///warehouse'
);

USE CATALOG paimon;

CREATE DATABASE IF NOT EXISTS ods;
CREATE DATABASE IF NOT EXISTS dwd;
CREATE DATABASE IF NOT EXISTS ads;

-- Phase 1: catalog + databases only. ODS/DWD/ADS tables land in later phases.
-- Smoke check (optional, after JAR is present):
--   SHOW DATABASES;
--   SHOW CATALOGS;
