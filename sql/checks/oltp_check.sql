-- =============================================================================
-- OLTP Database checks
-- =============================================================================
--
-- Purpose:
--  Validate schema and seed data after a clean database initialization.
--
-- Expected result:
--  All checks pass without errors.
--
-- Usage:
--  psql -U ecommerce -d ecommerce -f sql/checks/oltp_check.sql
--
-- =============================================================================

\set ON_ERROR_STOP on

-- =============================================================================
-- 1. Basic row counts
-- =============================================================================

DO $$
DECLARE
    actual_count bigint;
BEGIN
    select count(*) into actual_count from customers;

    IF actual_count <> 100 THEN
        RAISE EXCEPTION
            'CHECK FAILED: customers count is %, expected 100',
            actual_count;
    END IF;

    select count(*) into actual_count from products;

    IF actual_count <> 50 THEN
        RAISE EXCEPTION
            'CHECK FAILED: products count is %, expected 50',
            actual_count;
    END IF;

    select count(*) into actual_count from orders;

    IF actual_count <> 550 THEN
        RAISE EXCEPTION
            'CHECK FAILED: orders count is %, expected 550',
            actual_count;
    END IF;

END $$;

-- =============================================================================
-- 2. Order items
-- =============================================================================

DO $$
DECLARE
	invalid_count bigint;
BEGIN
	SELECT COUNT(*)
	INTO invalid_count
	FROM (
		select order_id
		from order_items
		group by order_id
		having count(*) < 1 or count(*) > 10
	) t;

	if invalid_count > 0 then
		raise exception
			'CHECK FAILED: % orders have invalid number of order items',
			invalid_count;
	end if;

    SELECT COUNT(*)
    INTO invalid_count
    FROM order_items
    WHERE quantity <= 0
       OR unit_price < 0;

    IF invalid_count > 0 THEN
        RAISE EXCEPTION
            'CHECK FAILED: % order_items have invalid quantity or price',
            invalid_count;
    END IF;
END $$;


-- =============================================================================
-- 3. Order items
-- =============================================================================

DO $$
DECLARE
    invalid_count BIGINT;
BEGIN
    SELECT COUNT(*)
    INTO invalid_count
    FROM (
        SELECT order_id
        FROM order_items
        GROUP BY order_id
        HAVING COUNT(*) < 1 OR COUNT(*) > 10
    ) t;

    IF invalid_count > 0 THEN
        RAISE EXCEPTION
            'CHECK FAILED: % orders have invalid number of order items',
            invalid_count;
    END IF;


    SELECT COUNT(*)
    INTO invalid_count
    FROM order_items
    WHERE quantity <= 0
       OR unit_price < 0;

    IF invalid_count > 0 THEN
        RAISE EXCEPTION
            'CHECK FAILED: % order_items have invalid quantity or price',
            invalid_count;
    END IF;
END $$;


-- =============================================================================
-- 4. Order totals
-- =============================================================================

DO $$
DECLARE
    invalid_count BIGINT;
BEGIN
    SELECT COUNT(*)
    INTO invalid_count
    FROM orders o
    JOIN (
        SELECT
            order_id,
            SUM(quantity * unit_price)::NUMERIC(12, 2) AS calculated_total
        FROM order_items
        GROUP BY order_id
    ) oi
        ON oi.order_id = o.id
    WHERE oi.calculated_total <> o.total_amount;

    IF invalid_count > 0 THEN
        RAISE EXCEPTION
            'CHECK FAILED: % orders have incorrect total_amount',
            invalid_count;
    END IF;
END $$;


-- =============================================================================
-- 5. Payment attempts
-- =============================================================================

DO $$
DECLARE
    invalid_count BIGINT;
BEGIN
    -- Every order must have between 1 and 3 payment attempts.
    SELECT COUNT(*)
    INTO invalid_count
    FROM (
        SELECT o.id
        FROM orders o
        LEFT JOIN payments p ON p.order_id = o.id
        GROUP BY o.id
        HAVING COUNT(p.id) < 1 OR COUNT(p.id) > 3
    ) t;

    IF invalid_count > 0 THEN
        RAISE EXCEPTION
            'CHECK FAILED: % orders have invalid number of payment attempts',
            invalid_count;
    END IF;


    -- Payment methods must not repeat within one order.
    SELECT COUNT(*)
    INTO invalid_count
    FROM (
        SELECT order_id
        FROM payments
        GROUP BY order_id
        HAVING COUNT(*) <> COUNT(DISTINCT payment_method)
    ) t;

    IF invalid_count > 0 THEN
        RAISE EXCEPTION
            'CHECK FAILED: % orders have duplicate payment methods',
            invalid_count;
    END IF;
END $$;


-- =============================================================================
-- 6. Successful payments
-- =============================================================================

DO $$
DECLARE
    invalid_count BIGINT;
BEGIN
    -- The last payment attempt must be successful.
    SELECT COUNT(*)
    INTO invalid_count
    FROM (
        SELECT
            p.order_id,
            p.status,
            ROW_NUMBER() OVER (
                PARTITION BY p.order_id
                ORDER BY p.created_at DESC, p.id DESC
            ) AS rn
        FROM payments p
    ) t
    WHERE rn = 1
      AND status <> 'successful';

    IF invalid_count > 0 THEN
        RAISE EXCEPTION
            'CHECK FAILED: % orders have a non-successful last payment',
            invalid_count;
    END IF;


    -- Successful payment amount must equal order total.
    SELECT COUNT(*)
    INTO invalid_count
    FROM orders o
    LEFT JOIN (
        SELECT
            order_id,
            SUM(amount) AS paid_amount
        FROM payments
        WHERE status = 'successful'
        GROUP BY order_id
    ) p
        ON p.order_id = o.id
    WHERE COALESCE(p.paid_amount, 0) <> o.total_amount;

    IF invalid_count > 0 THEN
        RAISE EXCEPTION
            'CHECK FAILED: % orders have incorrect successful payment amount',
            invalid_count;
    END IF;
END $$;


-- =============================================================================
-- 7. Status values
-- =============================================================================

DO $$
DECLARE
    invalid_count BIGINT;
BEGIN
    SELECT COUNT(*)
    INTO invalid_count
    FROM orders
    WHERE status NOT IN (
        'new',
        'paid',
        'processing',
        'shipped',
        'delivered',
        'cancelled'
    );

    IF invalid_count > 0 THEN
        RAISE EXCEPTION
            'CHECK FAILED: % orders have invalid status',
            invalid_count;
    END IF;


    SELECT COUNT(*)
    INTO invalid_count
    FROM payments
    WHERE status NOT IN (
        'pending',
        'failed',
        'successful'
    );

    IF invalid_count > 0 THEN
        RAISE EXCEPTION
            'CHECK FAILED: % payments have invalid status',
            invalid_count;
    END IF;
END $$;


-- =============================================================================
-- 8. Timestamp consistency
-- =============================================================================

DO $$
DECLARE
    invalid_count BIGINT;
BEGIN
    SELECT COUNT(*)
    INTO invalid_count
    FROM customers
    WHERE updated_at < created_at;

    IF invalid_count > 0 THEN
        RAISE EXCEPTION
            'CHECK FAILED: % customers have updated_at < created_at',
            invalid_count;
    END IF;


    SELECT COUNT(*)
    INTO invalid_count
    FROM products
    WHERE updated_at < created_at;

    IF invalid_count > 0 THEN
        RAISE EXCEPTION
            'CHECK FAILED: % products have updated_at < created_at',
            invalid_count;
    END IF;


    SELECT COUNT(*)
    INTO invalid_count
    FROM orders
    WHERE updated_at < created_at;

    IF invalid_count > 0 THEN
        RAISE EXCEPTION
            'CHECK FAILED: % orders have updated_at < created_at',
            invalid_count;
    END IF;


    SELECT COUNT(*)
    INTO invalid_count
    FROM payments
    WHERE updated_at < created_at;

    IF invalid_count > 0 THEN
        RAISE EXCEPTION
            'CHECK FAILED: % payments have updated_at < created_at',
            invalid_count;
    END IF;
END $$;


-- =============================================================================
-- 9. Number of orders per customer
-- =============================================================================

DO $$
DECLARE
    invalid_count BIGINT;
BEGIN
    SELECT COUNT(*)
    INTO invalid_count
    FROM (
        SELECT
            c.id,
            COUNT(o.id) AS order_count
        FROM customers c
        LEFT JOIN orders o
            ON o.customer_id = c.id
        GROUP BY c.id
        HAVING COUNT(o.id) < 1
            OR COUNT(o.id) > 10
    ) t;

    IF invalid_count > 0 THEN
        RAISE EXCEPTION
            'CHECK FAILED: % customers have invalid number of orders',
            invalid_count;
    END IF;
END $$;


-- =============================================================================
-- 10. Final result
-- =============================================================================

SELECT 'OLTP CHECKS PASSED' AS result;