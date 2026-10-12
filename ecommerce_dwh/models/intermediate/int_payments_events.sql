select
    payment_id,

    (case when op = 'd' then before_order_id else after_order_id end)
        as order_id,
    (case when op = 'd' then before_payment_method else after_payment_method end)
        as payment_method,
    (case when op = 'd' then before_status else after_status end)
        as status,
    (case when op = 'd' then before_amount else after_amount end)
        as amount,
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
from {{ ref('stg_payments_cdc') }}
where not is_tombstone