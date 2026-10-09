with transaction_events as (
    select
        *,
        row_number() over (
            partition by customer_id, source_tx_id
            order by source_lsn desc, kafka_offset desc
        ) as tx_rn
    from {{ ref('int_customers_events') }}
),

transaction_final as (
    select *
    from transaction_events
    where tx_rn = 1
),

ordered as (
    select
        *,
        row_number() over (
            partition by customer_id
            order by source_lsn, kafka_offset
        ) as rn,
        lag(email) over (
            partition by customer_id
            order by source_lsn, kafka_offset
        ) as prev_email,
        lag(first_name) over (
            partition by customer_id
            order by source_lsn, kafka_offset
        ) as prev_first_name,
        lag(last_name) over (
            partition by customer_id
            order by source_lsn, kafka_offset
        ) as prev_last_name,
        lag(is_deleted) over (
            partition by customer_id
            order by source_lsn, kafka_offset
        ) as prev_is_deleted
    from transaction_final
),

versions as (
    select
        *
    from ordered
    where rn = 1
        or email is distinct from prev_email
        or first_name is distinct from prev_first_name
        or last_name is distinct from prev_last_name
        or is_deleted is distinct from prev_is_deleted
),

valid_from_added as (
    select
        *,
        case
            when op = 'r' then created_at
            else source_ts
        end as valid_from
    from versions
),

valid_to_added as (
    select
        *,
        lead(valid_from) over (
            partition by customer_id
            order by source_lsn, kafka_offset
        ) as valid_to
    from valid_from_added
)

select
    md5(
        concat_ws(
            '|',
            customer_id::text,
            source_lsn::text
        )
    ) as customer_sk,
    customer_id,
	email,
	first_name,
	last_name,
    created_at as customer_created_at,
    is_deleted,
    valid_from,
    valid_to,
    valid_to is null as is_current
from valid_to_added
