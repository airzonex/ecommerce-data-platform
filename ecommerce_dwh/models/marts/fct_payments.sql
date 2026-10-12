select
    p.payment_id,
    p.order_id,
    o.customer_id,
    c.customer_sk,
    p.payment_method,
    p.status,
    d.date_sk as payment_date_sk,
    p.created_at as payment_created_at,
    p.amount,
    case when p.status = 'successful'
        then p.amount
        else 0
    end as successful_amount,
    p.is_deleted
from {{ ref('int_payments_current') }} as p

left join {{ ref('fct_orders') }} as o
    on o.order_id = p.order_id

left join {{ ref('dim_customers') }} as c
    on c.customer_id = o.customer_id
    and p.created_at >= c.valid_from
    and p.created_at < coalesce(c.valid_to, 'infinity'::timestamptz)

left join {{ ref('dim_date') }} as d
    on d.date = p.created_at::date