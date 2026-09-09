-- ChangeLake Phase 3: ODS Primary Key tables (current-state mirror)
-- PK matches MySQL source. merge-engine=deduplicate → latest logical row.
-- Phase 3: channel is added via ALTER TABLE ods.ods_orders ADD channel STRING
-- (see scripts/schema_evolution.sh). Baseline CREATE below has no channel.

USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS ods;
USE ods;

CREATE TABLE IF NOT EXISTS ods_users (
  user_id BIGINT,
  username STRING,
  city STRING,
  created_at TIMESTAMP(0),
  updated_at TIMESTAMP(0),
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'bucket' = '2',
  'changelog-producer' = 'input'
);

CREATE TABLE IF NOT EXISTS ods_orders (
  order_id BIGINT,
  user_id BIGINT,
  status STRING,
  amount DECIMAL(12, 2),
  order_ts TIMESTAMP(0),
  updated_at TIMESTAMP(0),
  -- channel STRING  -- added in G5 via ALTER (not in baseline CREATE)
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'bucket' = '2',
  'changelog-producer' = 'input'
);

CREATE TABLE IF NOT EXISTS ods_order_items (
  item_id BIGINT,
  order_id BIGINT,
  product_id BIGINT,
  qty INT,
  unit_price DECIMAL(12, 2),
  updated_at TIMESTAMP(0),
  PRIMARY KEY (item_id) NOT ENFORCED
) WITH (
  'bucket' = '2',
  'changelog-producer' = 'input'
);
