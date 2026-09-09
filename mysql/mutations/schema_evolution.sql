-- G5 Schema Evolution (Phase 3 minimum): ADD COLUMN channel
-- Idempotent: skip if column already exists (repeatable golden path).
-- Optional coupon_amount evolution is intentionally NOT applied here (later phase).
USE changelake;

SET @channel_exists := (
  SELECT COUNT(*)
  FROM information_schema.COLUMNS
  WHERE TABLE_SCHEMA = 'changelake'
    AND TABLE_NAME = 'orders'
    AND COLUMN_NAME = 'channel'
);

SET @ddl := IF(
  @channel_exists = 0,
  'ALTER TABLE orders ADD COLUMN channel VARCHAR(32) NULL',
  'SELECT ''channel already present'' AS schema_evolution'
);

PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
