select
    product_id
from {{ ref('dim_products') }}
where valid_to is not null 
  and valid_to <= valid_from