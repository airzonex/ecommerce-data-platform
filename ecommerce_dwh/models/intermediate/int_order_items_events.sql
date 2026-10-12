select
    order_item_id,

    (case when op = 'd' then before_order_id else after_order_id end)
        as order_id,
    (case when op = 'd' then before_product_id else after_product_id end)
        as product_id,
    (case when op = 'd' then before_quantity else after_quantity end)
        as quantity,
    (case when op = 'd' then before_unit_price else after_unit_price end)
        as unit_price,

    op,
    (op = 'd') as is_deleted,

    source_lsn,
    source_tx_id,
    source_ts,

    kafka_topic,
    kafka_partition,
    kafka_offset
from {{ ref('stg_order_items_cdc') }}
where not is_tombstone