-- The most damaging silent failure in a CDC pipeline: the warehouse quietly
-- holding a different number of entities than the source stream described.
--
-- Every distinct order_id in the event log must appear exactly once in
-- fct_orders. Catches a join fanning out, a filter dropping rows, and the
-- duplicate-generation bug where replaying a seed stacked extra events.

with cdc_orders as (

    select count(distinct pk) as n
    from {{ source('landing', 'cdc_events') }}
    where source_table = 'order'

),

fact_orders as (

    select count(*) as n from {{ ref('fct_orders') }}

)

select
    cdc_orders.n as cdc_order_count,
    fact_orders.n as fact_order_count
from cdc_orders
cross join fact_orders
where cdc_orders.n <> fact_orders.n
