-- ChangeLake Phase 5: DWD (business semantic freeze from ODS)
-- Table: dwd.dwd_orders — order-grain current state with net_amount.
-- coupon_amount is NOT in MySQL/ODS yet → stored NULL; net_amount = amount.
-- channel may be NULL for pre-G5 ODS rows (tolerated; ADS maps NULL → 'unknown').
-- PK = order_id; merge-engine=deduplicate; changelog-producer=input (for ADS consumers).
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
CREATE DATABASE IF NOT EXISTS dwd;
USE dwd;

CREATE TABLE IF NOT EXISTS dwd_orders (
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
  'bucket' = '2',
  'changelog-producer' = 'input',
  'merge-engine' = 'deduplicate'
);
