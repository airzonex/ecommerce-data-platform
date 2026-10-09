{{ config(materialized='table') }}

select distinct on (customer_id)
    customer_id,
    email,
    first_name,
    last_name,
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
    {{ ref('int_customers_events') }}
order by
    customer_id,
    source_lsn desc,
    kafka_offset desc
