with dates as (
    select d::date as date
    from generate_series(
	    '{{ var("dim_date_start") }}'::date,
	    '{{ var("dim_date_end") }}'::date,
	    interval '1 day'
    ) as d
)

select
    to_char(date, 'YYYYMMDD')::int as date_sk,
    date,
    extract(year from date)::int as year,
    extract(quarter from date)::int as quarter,
    extract(month from date)::int as month,
    to_char(date, 'FMMonth') as month_name,
    extract(week from date)::int as week,
    extract(day from date)::int as day_of_month,
    extract(isodow from date)::int as day_of_week,
    extract(isodow from date) in (6, 7) as is_weekend
from dates