-- G3: fixed UPDATE for Golden Path
USE changelake;

UPDATE orders
SET amount = 199.99,
    status = 'paid',
    updated_at = '2026-09-01 11:00:00'
WHERE order_id = 900001;
