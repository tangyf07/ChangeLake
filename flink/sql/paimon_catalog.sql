-- ChangeLake: Paimon filesystem catalog
-- Docs: https://paimon.apache.org/docs/1.4/flink/quick-start/
-- Requires: paimon-flink-1.18-1.4.2.jar (+ flink-shaded-hadoop uber) in Flink lib/

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'file:///warehouse'
);

USE CATALOG paimon;

CREATE DATABASE IF NOT EXISTS ods;
CREATE DATABASE IF NOT EXISTS dwd;
CREATE DATABASE IF NOT EXISTS ads;
