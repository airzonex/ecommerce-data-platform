UPDATE customers
SET first_name = 'Alicia'
WHERE id = :customer_id;

UPDATE products
SET price = 120
WHERE id = :product_id;

INSERT INTO payments (
    order_id,
    payment_method,
    status,
    amount
)
VALUES (
    :order_id,
    'card',
    'pending',
    200
)
RETURNING id as payment_id
\gset


SELECT :payment_id;