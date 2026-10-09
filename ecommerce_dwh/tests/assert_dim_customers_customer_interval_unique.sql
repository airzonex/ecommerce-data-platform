select
    customer_id,
    valid_from,
    count(*)
from {{ ref('dim_customers') }}
group by
    customer_id,
    valid_from
having count(*) > 1