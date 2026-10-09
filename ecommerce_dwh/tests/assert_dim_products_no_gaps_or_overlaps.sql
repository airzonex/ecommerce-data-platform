with next_valid_from_added as (
select
	product_id,
	valid_to,
	lead(valid_from) over (partition by product_id order by valid_from) as next_valid_from 
from {{ ref('dim_products') }}
)

select
	*
from next_valid_from_added
where valid_to is distinct from next_valid_from