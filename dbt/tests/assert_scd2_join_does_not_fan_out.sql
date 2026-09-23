-- The as-of join in fct_order_items must resolve each order to exactly one
-- customer version.
--
-- This is the failure SCD2 makes easy to introduce: if windows overlap, or the
-- range predicate uses closed bounds on both sides, an order matches two
-- versions and every line item is duplicated, inflating revenue. The row count
-- alone would look plausible.

with resolved as (

    select
        o.order_id,
        count(*) as matched_versions
    from {{ ref('int_orders_current') }} o
    join {{ ref('dim_customer') }} c
      on c.customer_unique_id = o.customer_unique_id
     and o.purchased_at >= c.valid_from
     and o.purchased_at <  c.valid_to
    group by 1

)

select order_id, matched_versions
from resolved
where matched_versions <> 1
