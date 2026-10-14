SET e2e.customer_id = :'customer_id';
SET e2e.product_id = :'product_id';
SET e2e.order_id = :'order_id';
SET e2e.payment_id = :'payment_id';
SET e2e.order_item_id = :'order_item_id';

DO $$
DECLARE
    old_customer_sk text;
    new_customer_sk text;
    old_first_name text;
    new_first_name text;
    old_valid_to timestamptz;
    new_valid_to timestamptz;
BEGIN
    WITH old_customer AS (
        SELECT
            customer_id,
            customer_sk,
            first_name,
            valid_to
        FROM marts.dim_customers
        WHERE customer_id = current_setting('e2e.customer_id')::bigint
          AND NOT is_current
          AND NOT is_deleted
    ),
    new_customer AS (
        SELECT
            customer_id,
            customer_sk,
            first_name,
            valid_to
        FROM marts.dim_customers
        WHERE customer_id = current_setting('e2e.customer_id')::bigint
          AND is_current
          AND NOT is_deleted
    )
    SELECT
        o.customer_sk,
        o.first_name,
        o.valid_to,
        n.customer_sk,
        n.first_name,
        n.valid_to
    INTO
        old_customer_sk, old_first_name, old_valid_to,
        new_customer_sk, new_first_name, new_valid_to
    FROM
        old_customer o
    JOIN new_customer n
        ON o.customer_id = n.customer_id;

    IF old_first_name IS DISTINCT FROM 'Alice'
        OR old_valid_to IS NULL
        OR new_first_name IS DISTINCT FROM 'Alicia'
        OR new_valid_to IS NOT NULL
        OR old_customer_sk IS NOT DISTINCT FROM new_customer_sk
    THEN
        RAISE EXCEPTION
            'CHECK FAILED: dim_customers has incorrect values: '
            'old_first_name: %, old_valid_to: %, '
            'new_first_name: %, new_valid_to: %',
            old_first_name, old_valid_to,
            new_first_name, new_valid_to;
    END IF;
END $$;

SELECT '  dim_customers is OK' AS result;

DO $$
DECLARE
    old_product_sk text;
    old_price numeric(12, 2);
    old_valid_to timestamptz;
    new_product_sk text;
    new_price numeric(12, 2);
    new_valid_to timestamptz;
BEGIN
    WITH old_product AS (
        SELECT
            product_id,
            product_sk,
            price,
            valid_to
        FROM marts.dim_products
        WHERE product_id = current_setting('e2e.product_id')::bigint
          AND NOT is_current
          AND NOT is_deleted
    ),
    new_product AS (
        SELECT
            product_id,
            product_sk,
            price,
            valid_to
        FROM marts.dim_products
        WHERE product_id = current_setting('e2e.product_id')::bigint
          AND is_current
          AND NOT is_deleted
    )
    SELECT
        o.product_sk,
        o.price,
        o.valid_to,
        n.product_sk,
        n.price,
        n.valid_to
    INTO
        old_product_sk, old_price, old_valid_to,
        new_product_sk, new_price, new_valid_to
    FROM
        old_product o
    JOIN new_product n
        ON o.product_id = n.product_id;

    IF old_price IS DISTINCT FROM 100
        OR old_valid_to IS NULL
        OR new_price IS DISTINCT FROM 120
        OR new_valid_to IS NOT NULL
        OR old_product_sk IS NOT DISTINCT FROM new_product_sk
    THEN
        RAISE EXCEPTION
            'CHECK FAILED: dim_products has incorrect values: '
            'old_price: %, old_valid_to: %, '
            'new_price: %, new_valid_to: %',
            old_price, old_valid_to,
            new_price, new_valid_to;
    END IF;
END $$;

SELECT '  dim_products is OK' as result;

DO $$
DECLARE
    first_name text;
BEGIN
    SELECT
        c.first_name
    INTO
        first_name
    FROM marts.fct_orders o
    JOIN marts.dim_customers c
        ON o.customer_sk = c.customer_sk
    WHERE order_id = current_setting('e2e.order_id')::bigint;

    IF first_name IS DISTINCT FROM 'Alice'
    THEN
        RAISE EXCEPTION
            'CHECK FAILED: incorrect first_name in fct_orders: '
            'expected: "Alice", actual: "%"',
            first_name;
    END IF;
END $$;

SELECT '  fct_orders is OK' AS result;

DO $$
DECLARE
    cust_first_name text;
    pay_method text;
    pay_status text;
    pay_amount numeric(12, 2);
    pay_successful_amount numeric(12, 2);
    pay_is_deleted boolean;
    pay_order_id bigint;
    pay_customer_id bigint;
BEGIN
    SELECT
        c.first_name,
        p.payment_method,
        p.status,
        p.amount,
        p.successful_amount,
        p.is_deleted,
        p.order_id,
        p.customer_id
    INTO
        cust_first_name,
        pay_method,
        pay_status,
        pay_amount,
        pay_successful_amount,
        pay_is_deleted,
        pay_order_id,
        pay_customer_id
    FROM marts.fct_payments p
    JOIN marts.dim_customers c
        ON p.customer_sk = c.customer_sk
    WHERE payment_id = current_setting('e2e.payment_id')::bigint;

    IF cust_first_name IS DISTINCT FROM 'Alicia'
        OR pay_method IS DISTINCT FROM 'card'
        OR pay_status IS DISTINCT FROM 'pending'
        OR pay_amount IS DISTINCT FROM 200
        OR pay_successful_amount IS DISTINCT FROM 0
        OR pay_is_deleted IS DISTINCT FROM false
        OR pay_order_id IS DISTINCT FROM current_setting('e2e.order_id')::bigint
        OR pay_customer_id IS DISTINCT FROM current_setting('e2e.customer_id')::bigint
    THEN
        RAISE EXCEPTION
            'CHECK FAILED: incorrect first_name in fct_payments: '
            'dim_customers.first_name: "%", '
            'fct_payments.payment_method: "%", '
            'fct_payments.status: "%", '
            'fct_payments.amount: %, '
            'fct_payments.successful_amount: %, '
            'fct_payments.is_deleted: %, '
            'fct_payments.order_id: %, '
            'fct_payments.customer_id: %',
            cust_first_name, pay_method, pay_status,
            pay_amount, pay_successful_amount,
            pay_is_deleted, pay_order_id,
            pay_customer_id;
    END IF;
END $$;

SELECT '  fct_payments is OK';

DO $$
DECLARE
    order_unit_price numeric(12, 2);
    prod_price numeric(12, 2);
BEGIN
    SELECT oi.unit_price, p.price
    INTO order_unit_price, prod_price 
    FROM marts.fct_order_items oi
    JOIN marts.dim_products p
        ON oi.product_sk = p.product_sk
    WHERE oi.order_item_id = current_setting('e2e.order_item_id')::bigint;

    IF order_unit_price IS DISTINCT FROM 100
        OR prod_price IS DISTINCT FROM 100
    THEN
        RAISE EXCEPTION
            'CHECK FAILED: incorrect prices in fct_order_items: '
            'unit_price from fct_order_items: %, price from dim_products: %',
            order_unit_price, prod_price;
    END IF;
END $$;

SELECT '  fct_order_items is OK';