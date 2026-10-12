{{ config(materialized='table') }}

select distinct on (payment_id)
    payment_id,
    order_id,
    payment_method,
    status,
    amount,
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
    {{ ref('int_payments_events') }}
order by
    payment_id,
    source_lsn desc,
    kafka_offset desc