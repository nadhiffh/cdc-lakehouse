-- One row per customer change event, typed.
--
-- event_seq is the ordering key for SCD2. It uses (lsn, kafka_offset) rather
-- than any business timestamp: snapshot reads have no LSN so they sort first
-- via the coalesce, and among streamed events LSN is monotonic in commit order
-- (verified: zero backwards LSNs, zero collisions across 295,531 events).

with events as (

    select
        pk                                        as customer_unique_id,
        op,
        is_snapshot,
        lsn,
        kafka_offset,
        tx_id,
        to_timestamp(event_ts_ms / 1000)          as event_at,

        json_extract_string(after,  '$.zip_code_prefix') as zip_code_prefix,
        json_extract_string(after,  '$.city')            as city,
        json_extract_string(after,  '$.state')           as state,

        -- Business instant the address became true, set explicitly by the
        -- seed/replay. This is the SCD2 window bound; event_ts_ms below is only
        -- when the connector observed the change.
        cast(json_extract(after, '$.updated_at') as bigint) as changed_ms,

        -- The pre-image. Populated because the source tables are set to
        -- REPLICA IDENTITY FULL; without it an address correction could not be
        -- distinguished from a relocation.
        json_extract_string(before, '$.zip_code_prefix') as prev_zip_code_prefix,
        json_extract_string(before, '$.city')            as prev_city,
        json_extract_string(before, '$.state')           as prev_state

    from {{ source('landing', 'cdc_events') }}
    where source_table = 'customer'

)

select
    customer_unique_id,
    op,
    is_snapshot,
    event_at,
    to_timestamp(changed_ms / 1000) as changed_at,
    zip_code_prefix,
    city,
    state,
    prev_zip_code_prefix,
    prev_city,
    prev_state,

    -- Snapshot reads carry no LSN; they represent the initial state and must
    -- sort before every streamed change for the same key.
    coalesce(lsn, 0)                              as lsn,
    kafka_offset,
    tx_id,

    row_number() over (
        partition by customer_unique_id
        order by coalesce(lsn, 0), kafka_offset
    )                                             as event_seq,

    -- True when the event did not actually change any tracked attribute. These
    -- must not open a new SCD2 version, or every unrelated column update would
    -- inflate the dimension.
    op = 'update'
        and prev_zip_code_prefix is not distinct from zip_code_prefix
        and prev_city            is not distinct from city
        and prev_state           is not distinct from state
                                                  as is_no_op_update

from events
