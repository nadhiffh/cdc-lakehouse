-- One row per order change event, typed.
--
-- Debezium sends every TIMESTAMP column here as epoch millis because the
-- connector runs with time.precision.mode=connect. That includes
-- estimated_delivery_at: the OLTP schema declares it TIMESTAMP rather than
-- DATE, so it arrives as millis like the rest (verified against the raw
-- envelope, where it reads 1533168000000 and not a small day count).

with events as (

    select
        pk                                    as order_id,
        op,
        is_snapshot,
        lsn,
        kafka_offset,
        tx_id,
        to_timestamp(event_ts_ms / 1000)      as event_at,

        json_extract_string(after, '$.customer_id')        as customer_id,
        json_extract_string(after, '$.customer_unique_id') as customer_unique_id,
        json_extract_string(after, '$.order_status')       as order_status,
        json_extract_string(before, '$.order_status')      as prev_order_status,

        -- Business instant of this transition, set by the replay. The SCD2
        -- window bound; event_ts_ms is only the connector's observation time.
        cast(json_extract(after, '$.updated_at') as bigint) as changed_ms,

        cast(json_extract(after, '$.purchased_at')          as bigint) as purchased_ms,
        cast(json_extract(after, '$.approved_at')           as bigint) as approved_ms,
        cast(json_extract(after, '$.delivered_carrier_at')  as bigint) as shipped_ms,
        cast(json_extract(after, '$.delivered_customer_at') as bigint) as delivered_ms,
        cast(json_extract(after, '$.estimated_delivery_at') as bigint) as estimated_ms

    from {{ source('landing', 'cdc_events') }}
    where source_table = 'order'

)

select
    order_id,
    customer_id,
    customer_unique_id,
    order_status,
    prev_order_status,
    op,
    is_snapshot,
    event_at,
    to_timestamp(changed_ms / 1000)    as changed_at,

    to_timestamp(purchased_ms / 1000)  as purchased_at,
    to_timestamp(approved_ms  / 1000)  as approved_at,
    to_timestamp(shipped_ms   / 1000)  as shipped_at,
    to_timestamp(delivered_ms / 1000)  as delivered_at,
    cast(to_timestamp(estimated_ms / 1000) as date) as estimated_delivery_date,

    coalesce(lsn, 0)                   as lsn,
    kafka_offset,
    tx_id,

    row_number() over (
        partition by order_id
        order by coalesce(lsn, 0), kafka_offset
    )                                  as event_seq,

    order_status is not distinct from prev_order_status
        and op = 'update'              as is_no_op_update

from events
