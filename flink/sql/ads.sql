-- ChangeLake Phase 5: ADS daily order metrics
-- Table: ads.ads_order_daily — metrics by (dt, channel).
-- NULL ODS/DWD channel is mapped to literal 'unknown' at write time (documented).
-- Metrics only: order_cnt, paid_order_cnt, gmv, net_gmv, buyer_cnt.
-- Paid status set: ('paid','shipped','completed') — see docs/dwd-ads.md.
-- Session: CREATE CATALOG every time (Flink has no IF NOT EXISTS for catalogs).

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
USE ads;

CREATE TABLE IF NOT EXISTS ads_order_daily (
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
