{{ config(materialized='table') }}

-- Grain: one row per order line item. 112,650 rows, reconciling 1:1 with the
-- source order_item table.
--
-- The point of interest is customer_key. It resolves to the customer version
-- that was valid when the CDC event stream first recorded the order, not the
-- customer's current address. That is what an SCD2 dimension is for: a revenue
-- report by state stays correct for orders placed before someone moved.
--
-- 775 orders have no line items at all, so they are absent here by design.
-- fct_orders covers them, and a test asserts the two reconcile.

with items as (

    select * from {{ ref('stg_order_items') }}

),

orders as (

    select * from {{ ref('int_orders_current') }}

),

-- Resolve the customer version valid at the time the order entered the stream.
-- The range is half-open: valid_from inclusive, valid_to exclusive, so an
-- event landing exactly on a boundary matches exactly one version.
customer_at_order as (

    select
        o.order_id,
        c.customer_key,
        c.version as customer_version
    from orders o
    join {{ ref('dim_customer') }} c
      on c.customer_unique_id = o.customer_unique_id
     and o.purchased_at >= c.valid_from
     and o.purchased_at <  c.valid_to

)

select
    {{ dbt_utils.generate_surrogate_key(['i.order_id', 'i.order_item_id']) }}
        as order_item_key,

    i.order_id,
    i.order_item_id,

    -- Dimension keys
    coalesce(cao.customer_key, '_unmatched') as customer_key,
    p.product_key,
    s.seller_key,

    o.customer_unique_id,
    o.order_status,

    i.price,
    i.freight_value,
    i.price + i.freight_value as item_total,

    o.purchased_at,
    o.approved_at,
    o.shipped_at,
    o.delivered_at,
    o.estimated_delivery_date,
    cast(o.purchased_at as date) as purchased_date,

    -- Delivered late against the estimate. Null when never delivered, so it is
    -- not silently counted as on-time.
    case
        when o.delivered_at is not null
        then cast(o.delivered_at as date) > o.estimated_delivery_date
    end as is_delivered_late,

    cao.customer_key is null as is_customer_version_unmatched

from items i
join orders o
  on o.order_id = i.order_id
left join customer_at_order cao
  on cao.order_id = i.order_id
left join {{ ref('dim_product') }} p
  on p.product_id = i.product_id
left join {{ ref('dim_seller') }} s
  on s.seller_id = i.seller_id
