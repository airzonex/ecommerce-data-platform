select
    order_id,

    (case when op = 'd' then before_customer_id else after_customer_id end)
        as customer_id,
    (case when op = 'd' then before_status else after_status end)
        as status,
    (case when op = 'd' then before_total_amount else after_total_amount end)
        as total_amount,
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
from {{ ref('stg_orders_cdc') }}
where not is_tombstone