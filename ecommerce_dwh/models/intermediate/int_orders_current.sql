{{ config(materialized='table') }}

select distinct on (order_id)
    order_id,
    customer_id,
    status,
    total_amount,
    created_at,
    updated_at,
    is_deleted,
    source_lsn,
    source_tx_id,
    source_ts,
    kafka_topic,
    kafka_partition,
    kafka_offset
from
    {{ ref('int_orders_events') }}
order by
    order_id,
    source_lsn desc,
    kafka_offset desc