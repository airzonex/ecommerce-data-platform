select
    (event_key ->> 'id')::bigint as product_id,

    event_payload #>> '{payload, before, name}'
        as before_name,
    event_payload #>> '{payload, before, category}'
        as before_category,
    (event_payload #>> '{payload, before, price}')::numeric(12, 2)
        as before_price,
    (event_payload #>> '{payload, before, created_at}')::timestamptz
        as before_created_at,
    (event_payload #>> '{payload, before, updated_at}')::timestamptz
        as before_updated_at,
    
    event_payload #>> '{payload, after, name}'
        as after_name,
    event_payload #>> '{payload, after, category}'
        as after_category,
    (event_payload #>> '{payload, after, price}')::numeric(12, 2)
        as after_price,
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
where kafka_topic = 'ecommerce.public.products'