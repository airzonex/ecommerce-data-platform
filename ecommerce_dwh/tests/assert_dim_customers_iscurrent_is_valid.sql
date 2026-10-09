select 
	customer_id
from {{ ref('dim_customers') }}
where
	    (is_current and valid_to is not null)
	or  (not is_current and valid_to is null)