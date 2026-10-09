select 
	product_id
from {{ ref('dim_products') }}
where
	    (is_current and valid_to is not null)
	or  (not is_current and valid_to is null)