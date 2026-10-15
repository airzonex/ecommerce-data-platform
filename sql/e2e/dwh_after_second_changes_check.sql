SET e2e.order_id = :'order_id';
SET e2e.payment_id = :'payment_id';

DO $$
DECLARE
    ord_status text;
BEGIN
    SELECT status
    INTO ord_status
    FROM marts.fct_orders o
    WHERE o.order_id = current_setting('e2e.order_id')::bigint;

    IF ord_status IS DISTINCT FROM 'paid'
    THEN
        RAISE EXCEPTION
            'CHECK FAILED: incorrect status in fct_orders: '
            'expected: "paid", actual: "%"',
            ord_status;
    END IF;
END $$;

SELECT '  fct_orders is OK';

DO $$
DECLARE
    pay_status text;
    pay_amount numeric(12, 2);
    pay_success_amount numeric(12, 2);
    pay_is_deleted boolean;
BEGIN
    SELECT
        status,
        amount,
        successful_amount,
        is_deleted
    INTO
        pay_status,
        pay_amount,
        pay_success_amount,
        pay_is_deleted
    FROM marts.fct_payments
    WHERE payment_id = current_setting('e2e.payment_id')::bigint;

    IF pay_status IS DISTINCT FROM 'successful'
        OR pay_success_amount IS DISTINCT FROM pay_amount
        OR pay_amount IS DISTINCT FROM 200
        OR pay_is_deleted IS DISTINCT FROM false
    THEN
        RAISE EXCEPTION
            'CHECK FAILED: fct_payments has incorrect values: '
            'fct_payments.status: "%", '
            'fct_payments.successful_amount: %, '
            'fct_payments.amount: %, '
            'fct_payments.is_deleted: %',
            pay_status, pay_success_amount,
            pay_amount, pay_is_deleted;
    END IF;
END $$;

SELECT '  fct_payments is OK';