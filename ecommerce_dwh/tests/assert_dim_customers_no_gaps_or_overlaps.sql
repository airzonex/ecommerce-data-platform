with next_valid_from_added as (
select
	customer_id,
	valid_to,
	lead(valid_from) over (partition by customer_id order by valid_from) as next_valid_from 
from {{ ref('dim_customers') }}
)

select
	*
from next_valid_from_added
where valid_to is distinct from next_valid_from