select
    (event_key ->> 'id')::bigint as customer_id,

    event_payload #>> '{payload, before, email}'
        as before_email,
    event_payload #>> '{payload, before, first_name}'
        as before_first_name,
    event_payload #>> '{payload, before, last_name}'
        as before_last_name,
    (event_payload #>> '{payload, before, created_at}')::timestamptz
        as before_created_at,
    (event_payload #>> '{payload, before, updated_at}')::timestamptz
        as before_updated_at,
    
    event_payload #>> '{payload, after, email}'
        as after_email,
    event_payload #>> '{payload, after, first_name}'
        as after_first_name,
    event_payload #>> '{payload, after, last_name}'
        as after_last_name,
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
where kafka_topic = 'ecommerce.public.customers'