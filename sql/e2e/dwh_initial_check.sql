SET e2e.customer_id = :'customer_id';
SET e2e.product_id = :'product_id';
SET e2e.order_id = :'order_id';
SET e2e.order_item_id = :'order_item_id';

DO $$
DECLARE
    expected_customer_sk text;
BEGIN
    SELECT
        customer_sk
    INTO
        expected_customer_sk
    FROM
        marts.dim_customers
    WHERE customer_id = current_setting('e2e.customer_id')::bigint
      AND first_name = 'Alice'
      AND is_current
      AND NOT is_deleted;

	if expected_customer_sk IS NULL then
		raise exception
			'CHECK FAILED: customer not found in dim_customers';
	end if;

    PERFORM set_config('e2e.customer_sk', expected_customer_sk, false);
END $$;

SELECT '  dim_customers is OK' AS result;

DO $$
DECLARE
    expected_product_sk text;
BEGIN
    SELECT
        product_sk
    INTO expected_product_sk
    FROM
        marts.dim_products
    WHERE product_id = current_setting('e2e.product_id')::bigint
      AND price = 100
      AND is_current
      AND NOT is_deleted;

    IF expected_product_sk IS NULL THEN
        RAISE EXCEPTION
            'CHECK FAILED: product not found in dim_products';
    END IF;

    PERFORM set_config('e2e.product_sk', expected_product_sk, false);
END $$;

SELECT '  dim_products is OK' AS result;

DO $$
DECLARE
    ord_status text;
    ord_cust_id bigint;
    ord_cust_sk text;
    ord_is_deleted boolean;
BEGIN
    SELECT
        status,
        customer_id,
        customer_sk,
        is_deleted
    INTO ord_status, ord_cust_id, ord_cust_sk, ord_is_deleted
    FROM
        marts.fct_orders
    WHERE order_id = current_setting('e2e.order_id')::bigint;

    IF ord_status IS DISTINCT FROM 'new'
        OR ord_cust_id IS DISTINCT FROM
            current_setting('e2e.customer_id')::bigint
        OR ord_cust_sk IS DISTINCT FROM
            current_setting('e2e.customer_sk')
        OR ord_is_deleted IS DISTINCT FROM false
    THEN
        RAISE EXCEPTION
            'CHECK FAILED: fct_orders has incorrect values: '
            'status = %, customer_id = %, customer_sk = %, is_deleted = %',
            ord_status, ord_cust_id, ord_cust_sk, ord_is_deleted;
    END IF;
END $$;

SELECT '  fct_orders is OK' AS result;

DO $$
DECLARE
    item_quantity integer;
    item_unit_price numeric(12, 2);
    item_line_amount numeric(12, 2);
    item_product_sk text;
    item_customer_sk text;
    item_order_id bigint;
    item_product_id bigint;
    item_is_deleted boolean;
BEGIN
    SELECT
        quantity,
        unit_price,
        line_amount,
        product_sk,
        customer_sk,
        order_id,
        product_id,
        is_deleted
    INTO item_quantity, item_unit_price,
        item_line_amount, item_product_sk,
        item_customer_sk, item_order_id,
        item_product_id, item_is_deleted
    FROM
        marts.fct_order_items
    WHERE order_item_id = current_setting('e2e.order_item_id')::bigint;
    
    IF item_quantity IS DISTINCT FROM 2
        OR item_unit_price IS DISTINCT FROM 100
        OR item_line_amount IS DISTINCT FROM 200
        OR item_product_sk IS DISTINCT FROM
            current_setting('e2e.product_sk')
        OR item_customer_sk IS DISTINCT FROM
            current_setting('e2e.customer_sk')
        OR item_order_id IS DISTINCT FROM
            current_setting('e2e.order_id')::bigint
        OR item_product_id IS DISTINCT FROM
            current_setting('e2e.product_id')::bigint
        OR item_is_deleted IS DISTINCT FROM false
    THEN
        RAISE EXCEPTION
            'CHECK FAILED: fct_order_items has invalid values: '
            'quantity = %, unit_price = %, '
            'line_amount = %, product_sk = %',
            item_quantity, item_unit_price,
            item_line_amount, item_product_sk;
    END IF;
END $$;

SELECT '  fct_order_items is OK' AS result;