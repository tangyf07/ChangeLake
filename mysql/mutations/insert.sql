-- G2: fixed INSERT for Golden Path
USE changelake;

INSERT INTO orders (order_id, user_id, status, amount, order_ts, updated_at)
VALUES (900001, 1, 'created', 99.50, '2026-09-01 10:00:00', '2026-09-01 10:00:00')
ON DUPLICATE KEY UPDATE
  user_id = VALUES(user_id),
  status = VALUES(status),
  amount = VALUES(amount),
  order_ts = VALUES(order_ts),
  updated_at = VALUES(updated_at);
