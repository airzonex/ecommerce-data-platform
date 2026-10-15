SET e2e.payment_id = :'payment_id';
SET e2e.order_id = :'order_id';

DO $$
DECLARE
    pay_order_id bigint;
    pay_status text;
    pay_amount numeric(12, 2);
    pay_success_amount numeric(12, 2);
    pay_is_deleted boolean;
BEGIN
    SELECT
        order_id,
        status,
        amount,
        successful_amount,
        is_deleted
    INTO
        pay_order_id,
        pay_status,
        pay_amount,
        pay_success_amount,
        pay_is_deleted
    FROM marts.fct_payments
    WHERE payment_id = current_setting('e2e.payment_id')::bigint;

    IF pay_is_deleted IS DISTINCT FROM true
        OR pay_status IS DISTINCT FROM 'successful'
        OR pay_amount IS DISTINCT FROM 200
        OR pay_success_amount IS DISTINCT FROM 200
        OR pay_order_id IS DISTINCT FROM current_setting('e2e.order_id')::bigint
    THEN
        RAISE EXCEPTION
            'CHECK FAILED: fct_payments has incorrect values: '
            'order_id: %, status: %, amount: %, '
            'successful_amount: %, is_deleted: %',
            pay_order_id, pay_status, pay_amount,
            pay_success_amount, pay_is_deleted;
    END IF;
END $$;

SELECT '  fct_payments is OK';