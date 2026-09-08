-- ChangeLake Phase 1: MySQL schema (no channel/coupon yet)
CREATE DATABASE IF NOT EXISTS changelake
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE changelake;

CREATE TABLE IF NOT EXISTS users (
  user_id BIGINT PRIMARY KEY,
  username VARCHAR(64) NOT NULL,
  city VARCHAR(64),
  created_at DATETIME NOT NULL,
  updated_at DATETIME NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS orders (
  order_id BIGINT PRIMARY KEY,
  user_id BIGINT NOT NULL,
  status VARCHAR(32) NOT NULL,
  amount DECIMAL(12,2) NOT NULL,
  order_ts DATETIME NOT NULL,
  updated_at DATETIME NOT NULL,
  KEY idx_orders_user_id (user_id),
  KEY idx_orders_order_ts (order_ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS order_items (
  item_id BIGINT PRIMARY KEY,
  order_id BIGINT NOT NULL,
  product_id BIGINT NOT NULL,
  qty INT NOT NULL,
  unit_price DECIMAL(12,2) NOT NULL,
  updated_at DATETIME NOT NULL,
  KEY idx_order_items_order_id (order_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
