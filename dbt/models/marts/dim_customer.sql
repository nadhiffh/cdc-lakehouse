{{ config(materialized='table') }}

-- SCD Type 2 customer dimension, built from the CDC event log.
--
-- Keyed on customer_unique_id, the real person. The source's customer_id is a
-- per-order alias (99,441 of them against 96,096 people) and would produce a
-- dimension that can never hold history, because each alias appears once.
--
-- 252 people genuinely relocate in this data: 246 with two versions, 5 with
-- three, 1 with four, from 259 address-change events replayed out of the source
-- rather than invented.
--
-- Window bounds come from changed_at, the business instant the address became
-- true, carried in customer.updated_at. Debezium's ts_ms is deliberately not
-- used: it records when the connector observed the event, and the replay
-- compresses two years of history into under a minute of wall clock, which
-- collapsed 189,737 windows to zero width when it was tried.
--
-- Version order still comes from (lsn, kafka_offset), the only strictly
-- monotonic key available.

with events as (

    select
        customer_unique_id,
        zip_code_prefix,
        city,
        state,
        changed_at,
        event_at,
        lsn,
        kafka_offset
    from {{ ref('stg_customer_events') }}
    -- A no-op update (same address written again) must not open a new version,
    -- or unrelated column touches would inflate the dimension.
    where not is_no_op_update

),

with_next as (

    select
        *,
        row_number() over w as version,
        count(*) over (partition by customer_unique_id) as total_versions,
        lead(changed_at) over w as next_changed_at
    from events
    window w as (partition by customer_unique_id order by lsn, kafka_offset)

)

select
    -- Surrogate key, unique per version, so a fact can point at the address a
    -- customer had at the time of their order rather than their current one.
    {{ dbt_utils.generate_surrogate_key(['customer_unique_id', 'version']) }}
        as customer_key,

    customer_unique_id,
    version,

    zip_code_prefix,
    city,
    state,

    changed_at as valid_from,
    -- Open-ended current version. A sentinel rather than NULL keeps as-of joins
    -- simple and lets the window-width test stay meaningful.
    coalesce(next_changed_at, timestamp '9999-12-31 23:59:59') as valid_to,
    next_changed_at is null as is_current,
    total_versions > 1      as has_history,

    -- When the connector observed the change. Kept for lineage and for
    -- measuring end-to-end CDC latency, never used as a window bound.
    event_at as cdc_observed_at

from with_next
