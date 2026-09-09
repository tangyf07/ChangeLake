-- ChangeLake Phase 2: MySQL CDC sources (Flink SQL connector mysql-cdc)
-- Connector: flink-sql-connector-mysql-cdc 3.1.1 (Flink 1.18)
-- Docs: https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/mysql-cdc/
-- Requires REPLICATION SLAVE/CLIENT on MySQL user (see mysql/003_cdc_grants.sql).
-- Distinct server-id ranges per table (must not overlap with MySQL server-id=1).

CREATE TABLE IF NOT EXISTS default_catalog.default_database.mysql_users (
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

CREATE TABLE IF NOT EXISTS default_catalog.default_database.mysql_orders (
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

CREATE TABLE IF NOT EXISTS default_catalog.default_database.mysql_order_items (
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
