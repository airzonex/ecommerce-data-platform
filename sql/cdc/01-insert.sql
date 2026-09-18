INSERT INTO products (
    name,
    category,
    price
)
VALUES (
    'CDC Test Product',
    'CDC Test',
    999.99
)
RETURNING *;