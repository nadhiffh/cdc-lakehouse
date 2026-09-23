{{ config(materialized='table') }}

-- SCD Type 2 over order status: one row per status an order held, with the
-- window it held it for. 96,531 orders pass through four states, 2,756 through
-- three, across 394,713 events.
--
-- Window bounds come from changed_at, the business instant of the transition,
-- which the replay writes into order.updated_at. Debezium's ts_ms is not used:
-- it records when the connector observed the event, and the replay pushes
-- 295,272 transitions through in about 48 seconds of wall clock, which
-- collapsed 189,737 windows to zero width when it was tried.
--
-- The business timestamps are not monotonic either. The source records 1,359
-- orders shipped before approved (worst case 171 days backwards) and 1,296
-- approved at the exact purchase instant. So:
--
--   * (lsn, kafka_offset) determines version order, being the only strictly
--     monotonic key.
--   * valid_from is the transition's own business instant, made non-decreasing
--     per order with a running max. A transition claiming to precede its
--     predecessor is clamped forward rather than dropped, and
--     is_timestamp_clamped records that it happened.
--   * Zero-length states are legitimate: an order really can be approved and
--     shipped at the same recorded instant. Flagged with is_instantaneous
--     rather than padded, so valid_to >= valid_from holds without inventing
--     duration.

with versioned as (

    select
        order_id,
        customer_unique_id,
        order_status,
        prev_order_status,
        changed_at,
        event_at,
        purchased_at,
        approved_at,
        shipped_at,
        delivered_at,
        lsn,
        kafka_offset
    from {{ ref('stg_order_events') }}
    where not is_no_op_update

),

ordered as (

    select
        *,
        row_number() over w as version,
        count(*) over (partition by order_id) as total_versions,

        -- Running max in commit order forces valid_from non-decreasing without
        -- discarding any event.
        max(changed_at) over (
            partition by order_id
            order by lsn, kafka_offset
            rows between unbounded preceding and current row
        ) as monotonic_at

    from versioned
    window w as (partition by order_id order by lsn, kafka_offset)

),

with_next as (

    select
        *,
        lead(monotonic_at) over (
            partition by order_id order by lsn, kafka_offset
        ) as next_at
    from ordered

)

select
    {{ dbt_utils.generate_surrogate_key(['order_id', 'version']) }}
        as order_status_key,

    order_id,
    customer_unique_id,
    version,

    order_status,
    prev_order_status,

    monotonic_at as valid_from,
    coalesce(next_at, timestamp '9999-12-31 23:59:59') as valid_to,
    next_at is null as is_current,

    case
        when next_at is not null
        then date_diff('second', monotonic_at, next_at)
    end as seconds_in_status,

    -- Began and ended at the same recorded instant. Real in this source rather
    -- than a modelling artefact, so flagged instead of padded.
    next_at is not null and next_at = monotonic_at as is_instantaneous,

    -- The source timestamp for this transition predated its predecessor and was
    -- clamped forward to keep the chain ordered.
    changed_at is not null and changed_at < monotonic_at as is_timestamp_clamped,

    changed_at as raw_changed_at,
    event_at   as cdc_observed_at,

    purchased_at,
    approved_at,
    shipped_at,
    delivered_at,

    -- Source defect flags, carried so the defect rate stays measurable.
    approved_at  is not null and approved_at < purchased_at  as is_approved_before_purchase,
    shipped_at   is not null and approved_at is not null
        and shipped_at < approved_at                         as is_shipped_before_approved,
    delivered_at is not null and shipped_at is not null
        and delivered_at < shipped_at                        as is_delivered_before_shipped,
    approved_at  is not null and approved_at = purchased_at  as is_approved_at_purchase_instant,

    total_versions as total_status_versions

from with_next
