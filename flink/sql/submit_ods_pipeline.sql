-- ChangeLake Phase 2: submit MySQL CDC → Paimon ODS (streaming statement set)
-- Pipeline name: changelake-ods-cdc
-- Runtime: streaming; checkpoint interval inherited from cluster (~30s)

SET 'pipeline.name' = 'changelake-ods-cdc';
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '30s';
SET 'table.exec.sink.upsert-materialize' = 'NONE';
SET 'parallelism.default' = '2';

-- Session-scoped: CREATE CATALOG every submit (Flink has no IF NOT EXISTS for catalogs).
CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'file:///warehouse'
);

USE CATALOG paimon;
CREATE DATABASE IF NOT EXISTS ods;

-- Recreate ODS sinks for a clean current-state mirror (demo-friendly)
DROP TABLE IF EXISTS ods.ods_users;
DROP TABLE IF EXISTS ods.ods_orders;
DROP TABLE IF EXISTS ods.ods_order_items;

CREATE TABLE ods.ods_users (
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

CREATE TABLE ods.ods_orders (
  order_id BIGINT,
  user_id BIGINT,
  status STRING,
  amount DECIMAL(12, 2),
  order_ts TIMESTAMP(0),
  updated_at TIMESTAMP(0),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'bucket' = '2',
  'changelog-producer' = 'input'
);

CREATE TABLE ods.ods_order_items (
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

USE CATALOG default_catalog;
USE default_database;

DROP TABLE IF EXISTS mysql_users;
DROP TABLE IF EXISTS mysql_orders;
DROP TABLE IF EXISTS mysql_order_items;

CREATE TABLE mysql_users (
  user_id BIGINT,
  username STRING,
  city STRING,
  created_at TIMESTAMP(0),
  updated_at TIMESTAMP(0),
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = 'mysql',
  'port' = '3306',
  'username' = 'changelake',
  'password' = 'changelake',
  'database-name' = 'changelake',
  'table-name' = 'users',
  'server-id' = '5401-5404',
  'server-time-zone' = 'UTC',
  'scan.startup.mode' = 'initial'
);

CREATE TABLE mysql_orders (
  order_id BIGINT,
  user_id BIGINT,
  status STRING,
  amount DECIMAL(12, 2),
  order_ts TIMESTAMP(0),
  updated_at TIMESTAMP(0),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = 'mysql',
  'port' = '3306',
  'username' = 'changelake',
  'password' = 'changelake',
  'database-name' = 'changelake',
  'table-name' = 'orders',
  'server-id' = '5405-5408',
  'server-time-zone' = 'UTC',
  'scan.startup.mode' = 'initial'
);

CREATE TABLE mysql_order_items (
  item_id BIGINT,
  order_id BIGINT,
  product_id BIGINT,
  qty INT,
  unit_price DECIMAL(12, 2),
  updated_at TIMESTAMP(0),
  PRIMARY KEY (item_id) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = 'mysql',
  'port' = '3306',
  'username' = 'changelake',
  'password' = 'changelake',
  'database-name' = 'changelake',
  'table-name' = 'order_items',
  'server-id' = '5409-5412',
  'server-time-zone' = 'UTC',
  'scan.startup.mode' = 'initial'
);

BEGIN STATEMENT SET;
INSERT INTO paimon.ods.ods_users
  SELECT user_id, username, city, created_at, updated_at FROM mysql_users;
INSERT INTO paimon.ods.ods_orders
  SELECT order_id, user_id, status, amount, order_ts, updated_at FROM mysql_orders;
INSERT INTO paimon.ods.ods_order_items
  SELECT item_id, order_id, product_id, qty, unit_price, updated_at FROM mysql_order_items;
END;
