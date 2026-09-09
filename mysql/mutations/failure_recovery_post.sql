-- G6 post-recovery mutations (after job RUNNING again)
USE changelake;

UPDATE orders
SET amount = 88.88,
    status = 'shipped',
    updated_at = '2026-09-01 12:10:00'
WHERE order_id = 900003;

UPDATE orders
SET amount = 302.22,
    status = 'shipped',
    updated_at = '2026-09-01 12:11:00'
WHERE order_id = 3;
