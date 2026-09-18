UPDATE products
SET price = 1099.99
WHERE name = 'CDC Test Product'
RETURNING *;