select
    customer_id
from {{ ref('dim_customers') }}
where valid_to is not null 
  and valid_to <= valid_from