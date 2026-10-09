select
    product_id
from {{ ref('dim_products') }}
group by product_id
having count(*) filter (where is_current) <> 1