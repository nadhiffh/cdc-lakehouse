{{ config(materialized='table') }}

-- Grain: one row per order. 99,441 rows, reconciling 1:1 with the source.
--
-- Exists alongside fct_order_items because 775 orders carry no line items.
-- Joining items to orders inner-style would drop them, and an order-count
-- metric taken from the item fact would silently be 0.8% low.

with orders as (

    select * from {{ ref('int_orders_current') }}

),

items as (

    select
        order_id,
        count(*)                  as item_count,
        count(distinct seller_id) as seller_count,
        count(distinct product_id) as product_count,
        sum(price)                as gross_price,
        sum(freight_value)        as gross_freight,
        sum(price + freight_value) as order_total
    from {{ ref('stg_order_items') }}
    group by 1

),

status_versions as (

    select
        order_id,
        max(total_status_versions) as status_versions,
        count_if(is_shipped_before_approved)       as inverted_ship_events,
        count_if(is_approved_at_purchase_instant)  as instant_approval_events
    from {{ ref('dim_order_status_history') }}
    group by 1

)

select
    o.order_id,
    o.customer_unique_id,
    o.order_status,

    o.purchased_at,
    o.approved_at,
    o.shipped_at,
    o.delivered_at,
    o.estimated_delivery_date,
    cast(o.purchased_at as date) as purchased_date,

    coalesce(i.item_count, 0)    as item_count,
    coalesce(i.seller_count, 0)  as seller_count,
    coalesce(i.product_count, 0) as product_count,
    i.gross_price,
    i.gross_freight,
    i.order_total,

    sv.status_versions,

    -- 775 orders genuinely have no line items. Flagged rather than filtered so
    -- the count stays visible and reconciliation against the source holds.
    i.order_id is null           as is_missing_items,

    o.order_status in ('delivered')                    as is_delivered,
    o.order_status in ('canceled', 'unavailable')      as is_terminated,

    case
        when o.delivered_at is not null
        then cast(o.delivered_at as date) > o.estimated_delivery_date
    end                                                as is_delivered_late,

    case
        when o.delivered_at is not null
        then date_diff('day', cast(o.purchased_at as date), cast(o.delivered_at as date))
    end                                                as days_to_deliver,

    coalesce(sv.inverted_ship_events, 0) > 0           as has_inverted_timestamps,
    coalesce(sv.instant_approval_events, 0) > 0        as has_instant_approval

from orders o
left join items i on i.order_id = o.order_id
left join status_versions sv on sv.order_id = o.order_id
