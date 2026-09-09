-- ChangeLake Phase 6 / G7: dt-scoped ADS rebuild (no double-count on re-run).
-- Placeholders: __DT__  (YYYY-MM-DD)
-- PK table (dt, channel): DELETE that dt, then INSERT aggregated metrics from DWD for dt only.
-- Paid set: ('paid','shipped','completed'); NULL channel → 'unknown'.
-- Does NOT full-wipe ADS; other dates untouched.

SET 'pipeline.name' = 'changelake-backfill-ads-__DT__';
SET 'execution.runtime-mode' = 'batch';
SET 'parallelism.default' = '1';
SET 'sql-client.execution.result-mode' = 'tableau';

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

-- Replace only the affected dt (logical partition key)
DELETE FROM ads.ads_order_daily WHERE dt = DATE '__DT__';

INSERT INTO ads.ads_order_daily
SELECT
  CAST(order_ts AS DATE) AS dt,
  COALESCE(channel, 'unknown') AS channel,
  CAST(COUNT(*) AS BIGINT) AS order_cnt,
  CAST(SUM(CASE WHEN status IN ('paid', 'shipped', 'completed') THEN 1 ELSE 0 END) AS BIGINT) AS paid_order_cnt,
  CAST(SUM(CASE WHEN status IN ('paid', 'shipped', 'completed') THEN amount ELSE CAST(0 AS DECIMAL(12, 2)) END) AS DECIMAL(18, 2)) AS gmv,
  CAST(SUM(CASE WHEN status IN ('paid', 'shipped', 'completed') THEN net_amount ELSE CAST(0 AS DECIMAL(12, 2)) END) AS DECIMAL(18, 2)) AS net_gmv,
  CAST(COUNT(DISTINCT CASE WHEN status IN ('paid', 'shipped', 'completed') THEN user_id END) AS BIGINT) AS buyer_cnt
FROM dwd.dwd_orders
WHERE CAST(order_ts AS DATE) = DATE '__DT__'
GROUP BY CAST(order_ts AS DATE), COALESCE(channel, 'unknown');
