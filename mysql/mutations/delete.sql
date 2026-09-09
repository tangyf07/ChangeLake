-- G4: fixed DELETE for Golden Path
USE changelake;

DELETE FROM orders
WHERE order_id = 900001;
