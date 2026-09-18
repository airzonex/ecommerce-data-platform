DELETE FROM products
WHERE name = 'CDC Test Product'
RETURNING *;