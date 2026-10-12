select
    o.order_id,
    c.customer_sk as customer_sk,
    o.customer_id,
    o.status,
    o.total_amount,
    d.date_sk as order_date_sk,
    o.created_at as order_created_at,
    o.updated_at as order_updated_at,
    o.is_deleted
from {{ ref('int_orders_current') }} o
left join {{ ref('dim_customers') }} c
    on o.customer_id = c.customer_id
    and o.created_at >= c.valid_from
    and o.created_at < coalesce(c.valid_to, 'infinity'::timestamptz)
left join {{ ref('dim_date') }} d
    on d.date = o.created_at::date