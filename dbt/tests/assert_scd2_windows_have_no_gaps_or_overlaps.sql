-- SCD2 validity windows for one key must tile time contiguously: each version's
-- valid_to is exactly the next version's valid_from.
--
-- A gap means a point in time where the entity had no row at all, so an
-- as-of join silently returns nothing. An overlap means two versions are
-- simultaneously valid, so the same join returns two rows and doubles measures.
-- Neither shows up in a uniqueness test.

with customer_chain as (

    select
        'dim_customer' as model,
        customer_unique_id as business_key,
        version,
        valid_to,
        lead(valid_from) over (
            partition by customer_unique_id order by version
        ) as next_valid_from
    from {{ ref('dim_customer') }}

),

order_chain as (

    select
        'dim_order_status_history' as model,
        order_id as business_key,
        version,
        valid_to,
        lead(valid_from) over (
            partition by order_id order by version
        ) as next_valid_from
    from {{ ref('dim_order_status_history') }}

),

combined as (

    select * from customer_chain
    union all
    select * from order_chain

)

select
    model,
    business_key,
    version,
    valid_to,
    next_valid_from
from combined
where next_valid_from is not null
  and valid_to <> next_valid_from
