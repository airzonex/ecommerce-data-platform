{{ config(materialized='table') }}

select distinct on (product_id)
    product_id,
    name,
    category,
    price,
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
    {{ ref('int_products_events') }}
order by
    product_id,
    source_lsn desc,
    kafka_offset desc