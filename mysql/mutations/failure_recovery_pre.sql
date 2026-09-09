-- G6 pre-kill mutations (after ≥1 Flink checkpoint)
USE changelake;

-- Dedicated G6 row (idempotent); works with or without orders.channel
INSERT INTO orders (order_id, user_id, status, amount, order_ts, updated_at)
VALUES (900003, 1, 'created', 10.00, '2026-09-01 12:00:00', '2026-09-01 12:00:00')
ON DUPLICATE KEY UPDATE
  user_id = VALUES(user_id),
  status = VALUES(status),
  amount = VALUES(amount),
  order_ts = VALUES(order_ts),
  updated_at = VALUES(updated_at);

-- Seed row mutation before TM kill
UPDATE orders
SET amount = 301.11,
    status = 'paid',
    updated_at = '2026-09-01 12:01:00'
WHERE order_id = 3;
