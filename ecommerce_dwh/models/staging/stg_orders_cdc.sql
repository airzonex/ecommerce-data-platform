select
    (event_key ->> 'id')::bigint as order_id,

    (event_payload #>> '{payload, before, customer_id}')::bigint
        as before_customer_id,
    event_payload #>> '{payload, before, status}'
        as before_status,
    (event_payload #>> '{payload, before, total_amount}')::numeric(12, 2)
        as before_total_amount,
    (event_payload #>> '{payload, before, created_at}')::timestamptz
        as before_created_at,
    (event_payload #>> '{payload, before, updated_at}')::timestamptz
        as before_updated_at,
    
    (event_payload #>> '{payload, after, customer_id}')::bigint
        as after_customer_id,
    event_payload #>> '{payload, after, status}'
        as after_status,
    (event_payload #>> '{payload, after, total_amount}')::numeric(12, 2)
        as after_total_amount,
    (event_payload #>> '{payload, after, created_at}')::timestamptz
        as after_created_at,
    (event_payload #>> '{payload, after, updated_at}')::timestamptz
        as after_updated_at,
    
    op,
    is_tombstone,
    source_lsn,
    source_tx_id,
    source_ts,
    kafka_topic,
    kafka_partition,
    kafka_offset
from {{ source('raw', 'cdc_events') }}
where kafka_topic = 'ecommerce.public.orders'
