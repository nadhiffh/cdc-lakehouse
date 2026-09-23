-- fct_orders.item_count must agree with the actual rows in fct_order_items.
--
-- Guards the 775 orders that have no line items. An inner join would drop them
-- from the item fact, which is correct, but it must not also silently change
-- the item counts carried on the order fact.

with from_order_fact as (

    select
        order_id,
        item_count as declared
    from {{ ref('fct_orders') }}

),

from_item_fact as (

    select
        order_id,
        count(*) as actual
    from {{ ref('fct_order_items') }}
    group by 1

)

select
    o.order_id,
    o.declared,
    coalesce(i.actual, 0) as actual
from from_order_fact o
left join from_item_fact i on i.order_id = o.order_id
where o.declared <> coalesce(i.actual, 0)
