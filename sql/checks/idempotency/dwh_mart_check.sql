SET idmp.product_id = :'product_id';

DO $$
DECLARE
    prod_count integer;
BEGIN
    SELECT COUNT(*)
    INTO prod_count
    FROM marts.dim_products
    WHERE product_id = current_setting('idmp.product_id')::bigint;

    IF prod_count IS DISTINCT FROM 1
    THEN
        RAISE EXCEPTION
            'CHECK FAILED: dim_products has % rows (expected 1)',
            prod_count;
    END IF;
END $$;

SELECT '  dim_products is OK' as result;