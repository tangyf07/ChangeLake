-- ChangeLake Phase 3: CDC → Paimon ODS AFTER schema evolution (channel present)
-- Does NOT drop existing ODS tables (preserves lake data / evolved schema).
-- Flink SQL mysql-cdc has a FIXED table schema at submit time — transparent
-- runtime ADD COLUMN is NOT supported on this connector (Pipeline YAML only).
-- Explicit migration: ALTER MySQL + ALTER Paimon + resubmit this job.
-- Docs: docs/schema-evolution.md

SET 'pipeline.name' = 'changelake-ods-cdc';
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '30s';
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
CREATE DATABASE IF NOT EXISTS ods;

-- Ensure channel exists on ods_orders (Paimon ADD COLUMN). Failures if already
-- present are handled by scripts/schema_evolution.sh before submit when needed.
-- Here we only CREATE IF NOT EXISTS for a cold start that already has channel
-- in MySQL (rare); normal G5 path ALTERs first then submits.

CREATE TABLE IF NOT EXISTS ods.ods_users (
  user_id BIGINT,
  username STRING,
  city STRING,
  created_at TIMESTAMP(0),
  updated_at TIMESTAMP(0),
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'bucket' = '1',
  'changelog-producer' = 'input'
);

CREATE TABLE IF NOT EXISTS ods.ods_orders (
  order_id BIGINT,
  user_id BIGINT,
  status STRING,
  amount DECIMAL(12, 2),
  order_ts TIMESTAMP(0),
  updated_at TIMESTAMP(0),
  channel STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'bucket' = '1',
  'changelog-producer' = 'input'
);

CREATE TABLE IF NOT EXISTS ods.ods_order_items (
  item_id BIGINT,
  order_id BIGINT,
  product_id BIGINT,
  qty INT,
  unit_price DECIMAL(12, 2),
  updated_at TIMESTAMP(0),
  PRIMARY KEY (item_id) NOT ENFORCED
) WITH (
  'bucket' = '1',
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

-- Evolved source schema: channel included (must match MySQL after ALTER)
CREATE TABLE mysql_orders (
  order_id BIGINT,
  user_id BIGINT,
  status STRING,
  amount DECIMAL(12, 2),
  order_ts TIMESTAMP(0),
  updated_at TIMESTAMP(0),
  channel STRING,
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
  SELECT order_id, user_id, status, amount, order_ts, updated_at, channel FROM mysql_orders;
INSERT INTO paimon.ods.ods_order_items
  SELECT item_id, order_id, product_id, qty, unit_price, updated_at FROM mysql_order_items;
END;
