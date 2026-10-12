select
    oi.order_item_id,
    oi.order_id,
    o.customer_sk,
    o.customer_id,
    p.product_sk,
    oi.product_id,
    o.order_date_sk,
    o.order_created_at,
    oi.quantity,
    oi.unit_price,
    oi.quantity * oi.unit_price as line_amount,
    oi.is_deleted
from {{ ref('int_order_items_current') }} oi
left join {{ ref('fct_orders') }} as o
    on oi.order_id = o.order_id
left join {{ ref('dim_products') }} as p
    on oi.product_id = p.product_id
    and o.order_created_at >= p.valid_from
    and o.order_created_at < coalesce(p.valid_to, 'infinity'::timestamptz)
