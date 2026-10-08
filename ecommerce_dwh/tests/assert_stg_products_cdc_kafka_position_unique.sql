select
    kafka_topic,
    kafka_partition,
    kafka_offset,
    count(*) as record_count
from {{ ref('stg_products_cdc') }}
group by
    kafka_topic,
    kafka_partition,
    kafka_offset
having count(*) > 1