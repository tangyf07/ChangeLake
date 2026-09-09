-- ChangeLake Phase 5: ODS → DWD streaming pipeline
-- Pipeline name: changelake-dwd-orders
-- Requires ods.ods_orders with channel column (post-G5 / evolved ODS).
-- coupon_amount not in ODS → CAST(NULL AS DECIMAL(12,2)); net_amount = amount.
-- Pre-G5 rows with NULL channel are preserved as NULL in DWD (ADS maps to 'unknown').
-- Source hint scan.mode=latest-full: emit current ODS snapshot then continue with changelog.
-- Checkpoint interval 10s matches cluster conf (faster DWD commit visibility before ADS).

SET 'pipeline.name' = 'changelake-dwd-orders';
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10s';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'parallelism.default' = '1';

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = 's3://changelake/warehouse',
  's3.endpoint' = 'http://minio:9000',
  's3.access-key' = 'minioadmin',
  's3.secret-key' = 'minioadmin',
  's3.path.style.access' = 'true'
);

USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS dwd;

DROP TABLE IF EXISTS dwd.dwd_orders;

CREATE TABLE dwd.dwd_orders (
  order_id BIGINT,
  user_id BIGINT,
  status STRING,
  amount DECIMAL(12, 2),
  channel STRING,
  coupon_amount DECIMAL(12, 2),
  net_amount DECIMAL(12, 2),
  order_ts TIMESTAMP(0),
  updated_at TIMESTAMP(0),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'bucket' = '1',
  'changelog-producer' = 'input',
  'merge-engine' = 'deduplicate'
);

INSERT INTO dwd.dwd_orders
SELECT
  order_id,
  user_id,
  status,
  amount,
  channel,
  CAST(NULL AS DECIMAL(12, 2)) AS coupon_amount,
  amount AS net_amount,
  order_ts,
  updated_at
FROM ods.ods_orders /*+ OPTIONS('scan.mode'='latest-full') */;
