-- G5 post-evolution DML (requires channel column on orders)
USE changelake;

-- Existing pre-evolution row: set channel
UPDATE orders
SET channel = 'app',
    updated_at = '2026-09-09 12:00:00'
WHERE order_id = 1;

-- New row with channel populated at insert time
INSERT INTO orders (order_id, user_id, status, amount, order_ts, updated_at, channel)
VALUES (900002, 2, 'created', 55.00, '2026-09-09 12:05:00', '2026-09-09 12:05:00', 'web')
ON DUPLICATE KEY UPDATE
  user_id = VALUES(user_id),
  status = VALUES(status),
  amount = VALUES(amount),
  order_ts = VALUES(order_ts),
  updated_at = VALUES(updated_at),
  channel = VALUES(channel);
