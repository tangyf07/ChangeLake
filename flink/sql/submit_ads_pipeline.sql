-- ChangeLake Phase 5: DWD → ADS daily metrics (batch refresh)
-- Pipeline name: changelake-ads-order-daily
-- Phase 5 uses bounded/batch INSERT OVERWRITE for correctness & re-runnability
-- on Flink 1.18.1 + Paimon 1.4.2 (streaming window agg is fragile for this demo).
-- NULL channel → literal 'unknown' (documented in docs/dwd-ads.md).
-- Paid status set: ('paid','shipped','completed').
-- gmv / net_gmv / paid_order_cnt / buyer_cnt counted only for paid statuses;
-- order_cnt counts all orders in the (dt, channel) group.

SET 'pipeline.name' = 'changelake-ads-order-daily';
SET 'execution.runtime-mode' = 'batch';
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
CREATE DATABASE IF NOT EXISTS ads;

CREATE TABLE IF NOT EXISTS ads.ads_order_daily (
  dt DATE,
  channel STRING,
  order_cnt BIGINT,
  paid_order_cnt BIGINT,
  gmv DECIMAL(18, 2),
  net_gmv DECIMAL(18, 2),
  buyer_cnt BIGINT,
  PRIMARY KEY (dt, channel) NOT ENFORCED
) WITH (
  'bucket' = '1',
  'merge-engine' = 'deduplicate',
  'changelog-producer' = 'input'
);

-- Full refresh of daily metrics from current DWD state
INSERT OVERWRITE ads.ads_order_daily
SELECT
  CAST(order_ts AS DATE) AS dt,
  COALESCE(channel, 'unknown') AS channel,
  CAST(COUNT(*) AS BIGINT) AS order_cnt,
  CAST(SUM(CASE WHEN status IN ('paid', 'shipped', 'completed') THEN 1 ELSE 0 END) AS BIGINT) AS paid_order_cnt,
  CAST(SUM(CASE WHEN status IN ('paid', 'shipped', 'completed') THEN amount ELSE CAST(0 AS DECIMAL(12, 2)) END) AS DECIMAL(18, 2)) AS gmv,
  CAST(SUM(CASE WHEN status IN ('paid', 'shipped', 'completed') THEN net_amount ELSE CAST(0 AS DECIMAL(12, 2)) END) AS DECIMAL(18, 2)) AS net_gmv,
  CAST(COUNT(DISTINCT CASE WHEN status IN ('paid', 'shipped', 'completed') THEN user_id END) AS BIGINT) AS buyer_cnt
FROM dwd.dwd_orders
GROUP BY CAST(order_ts AS DATE), COALESCE(channel, 'unknown');
