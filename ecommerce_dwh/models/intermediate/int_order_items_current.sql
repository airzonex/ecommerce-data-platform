{{ config(materialized='table') }}

select distinct on (order_item_id)
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    is_deleted,
    source_lsn,
    source_tx_id,
    source_ts,
    kafka_topic,
    kafka_partition,
    kafka_offset
from
    {{ ref('int_order_items_events') }}
order by
    order_item_id,
    source_lsn desc,
    kafka_offset desc