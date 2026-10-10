with next_date_added as (
select
	date,
	lead(date) over (order by date) as next_date
from {{ ref('dim_date') }}
order by date_sk desc
)

select
    *
from next_date_added 
where next_date is not null 
  and next_date <> date + 1