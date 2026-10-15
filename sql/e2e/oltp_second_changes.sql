UPDATE orders
SET status = 'paid'
WHERE id = :order_id;

UPDATE payments
SET status = 'successful'
WHERE id = :payment_id;