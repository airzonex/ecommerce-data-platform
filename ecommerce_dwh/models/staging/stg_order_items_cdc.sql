select
    (event_key ->> 'id')::bigint as order_item_id,

    (event_payload #>> '{payload, before, order_id}')::bigint
        as before_order_id,
    (event_payload #>> '{payload, before, product_id}')::bigint
        as before_product_id,
    (event_payload #>> '{payload, before, quantity}')::integer
        as before_quantity,
    (event_payload #>> '{payload, before, unit_price}')::numeric(12, 2)
        as before_unit_price,
    
    (event_payload #>> '{payload, after, order_id}')::bigint
        as after_order_id,
    (event_payload #>> '{payload, after, product_id}')::bigint
        as after_product_id,
    (event_payload #>> '{payload, after, quantity}')::integer
        as after_quantity,
    (event_payload #>> '{payload, after, unit_price}')::numeric(12, 2)
        as after_unit_price,
    
    op,
    is_tombstone,
    source_lsn,
    source_tx_id,
    source_ts,
    kafka_topic,
    kafka_partition,
    kafka_offset
from {{ source('raw', 'cdc_events') }}
where kafka_topic = 'ecommerce.public.order_items'
