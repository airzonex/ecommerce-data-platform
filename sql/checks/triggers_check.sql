\set ON_ERROR_STOP on

-- =============================================================================
-- updated_at trigger checks
-- =============================================================================
--
-- Verifies that the update_updated_at() trigger function correctly updates
-- updated_at on every table where the trigger is defined.
--
-- Tables:
--   customers
--   products
--   orders
--   payments
--
-- Important:
--   INSERT and UPDATE are executed as separate transactions because psql
--   runs each statement in autocommit mode.
--
-- =============================================================================


-- =============================================================================
-- 1. customers
-- =============================================================================

INSERT INTO customers (
    email,
    first_name,
    last_name
)
VALUES (
    'trigger-check-customers@example.com',
    'Trigger',
    'Check'
)
RETURNING
    id AS customer_id,
    updated_at AS initial_updated_at
\gset

SELECT pg_sleep(0.1);

UPDATE customers
SET first_name = 'Updated'
WHERE id = :customer_id
RETURNING updated_at AS updated_at
\gset

SELECT (
    :'updated_at'::timestamptz > :'initial_updated_at'::timestamptz
) AS trigger_check_passed
\gset

\if :trigger_check_passed
\else
    \echo 'CHECK FAILED: customers.updated_at was not updated'
    \quit 1
\endif

DELETE FROM customers
WHERE id = :customer_id;


-- =============================================================================
-- 2. products
-- =============================================================================

INSERT INTO products (
    name,
    category,
    price
)
VALUES (
    'Trigger Check Product',
    'Test',
    1.00
)
RETURNING
    id AS product_id,
    updated_at AS initial_updated_at
\gset

SELECT pg_sleep(0.1);

UPDATE products
SET price = 2.00
WHERE id = :product_id
RETURNING updated_at AS updated_at
\gset

SELECT (
    :'updated_at'::timestamptz > :'initial_updated_at'::timestamptz
) AS trigger_check_passed
\gset

\if :trigger_check_passed
\else
    \echo 'CHECK FAILED: products.updated_at was not updated'
    \quit 1
\endif

DELETE FROM products
WHERE id = :product_id;


-- =============================================================================
-- 3. orders
-- =============================================================================

INSERT INTO orders (
    customer_id,
    status,
    total_amount
)
SELECT
    id,
    'new',
    100.00
FROM customers
ORDER BY id
LIMIT 1
RETURNING
    id AS order_id,
    updated_at AS initial_updated_at
\gset

SELECT pg_sleep(0.1);

UPDATE orders
SET status = 'paid'
WHERE id = :order_id
RETURNING updated_at AS updated_at
\gset

SELECT (
    :'updated_at'::timestamptz > :'initial_updated_at'::timestamptz
) AS trigger_check_passed
\gset

\if :trigger_check_passed
\else
    \echo 'CHECK FAILED: orders.updated_at was not updated'
    \quit 1
\endif

DELETE FROM orders
WHERE id = :order_id;


-- =============================================================================
-- 4. payments
-- =============================================================================

INSERT INTO payments (
    order_id,
    payment_method,
    status,
    amount
)
SELECT
    id,
    'card',
    'pending',
    100.00
FROM orders
ORDER BY id
LIMIT 1
RETURNING
    id AS payment_id,
    updated_at AS initial_updated_at
\gset

SELECT pg_sleep(0.1);

UPDATE payments
SET status = 'successful'
WHERE id = :payment_id
RETURNING updated_at AS updated_at
\gset

SELECT (
    :'updated_at'::timestamptz > :'initial_updated_at'::timestamptz
) AS trigger_check_passed
\gset

\if :trigger_check_passed
\else
    \echo 'CHECK FAILED: payments.updated_at was not updated'
    \quit 1
\endif

DELETE FROM payments
WHERE id = :payment_id;


-- =============================================================================
-- Final result
-- =============================================================================

SELECT 'UPDATED_AT TRIGGER CHECKS PASSED' AS result;