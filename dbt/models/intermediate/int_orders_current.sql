-- Current state of each order: the last event per order_id.
--
-- Separate from dim_order_status_history because the fact table needs one row
-- per order, not one per status version. Deriving it from the same event log
-- keeps the two consistent by construction.

with ranked as (

    select
        *,
        row_number() over (
            partition by order_id
            order by lsn desc, kafka_offset desc
        ) as rn
    from {{ ref('stg_order_events') }}

)

select
    order_id,
    customer_id,
    customer_unique_id,
    order_status,
    purchased_at,
    approved_at,
    shipped_at,
    delivered_at,
    estimated_delivery_date,
    event_at as last_changed_at
from ranked
where rn = 1
