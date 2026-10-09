select
    product_id,
    valid_from,
    count(*)
from {{ ref('dim_products') }}
group by
    product_id,
    valid_from
having count(*) > 1