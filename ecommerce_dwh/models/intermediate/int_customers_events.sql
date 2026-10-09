select
    customer_id,

    (case when op = 'd' then before_email else after_email end)
        as email,
    (case when op = 'd' then before_first_name else after_first_name end)
        as first_name,
    (case when op = 'd' then before_last_name else after_last_name end)
        as last_name,
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
from {{ ref('stg_customers_cdc') }}
where not is_tombstone
