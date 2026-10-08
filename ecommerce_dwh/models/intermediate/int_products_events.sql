select
    product_id,

    (case when op = 'd' then before_name else after_name end)
        as name,
    (case when op = 'd' then before_category else after_category end)
        as category,
    (case when op = 'd' then before_price else after_price end)
        as price,
    (case when op = 'd' then before_created_at else after_created_at end)
        as created_at,
    (case when op = 'd' then before_updated_at else after_updated_at end)
        as updated_at,

    op,
    (op = 'd') as is_deleted,

    source_lsn,
    source_tx_id,
    source_ts,

    kafka_topic,
    kafka_partition,
    kafka_offset
from {{ ref('stg_products_cdc') }}
where not is_tombstone