-- The defining invariant of SCD Type 2: every business key has exactly one
-- current version. Zero means history closed without opening a successor;
-- more than one means a consumer joining on is_current fans out and silently
-- double-counts revenue.
--
-- Covers both SCD2 dimensions in one test.

with customer_current as (

    select
        'dim_customer' as model,
        customer_unique_id as business_key,
        count_if(is_current) as current_versions
    from {{ ref('dim_customer') }}
    group by 1, 2

),

order_current as (

    select
        'dim_order_status_history' as model,
        order_id as business_key,
        count_if(is_current) as current_versions
    from {{ ref('dim_order_status_history') }}
    group by 1, 2

)

select * from customer_current where current_versions <> 1
union all
select * from order_current where current_versions <> 1
