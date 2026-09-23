-- SCD2 version counts must be derivable from the event log, not invented by
-- the model.
--
-- For each customer, the number of versions must equal the number of events
-- that actually changed a tracked attribute. If the model dropped a change or
-- opened a spurious version on a no-op update, this diverges. A plain row count
-- on the dimension would not notice either.

with events_per_customer as (

    select
        customer_unique_id,
        count(*) as changing_events
    from {{ ref('stg_customer_events') }}
    where not is_no_op_update
    group by 1

),

versions_per_customer as (

    select
        customer_unique_id,
        count(*) as versions
    from {{ ref('dim_customer') }}
    group by 1

)

select
    e.customer_unique_id,
    e.changing_events,
    v.versions
from events_per_customer e
full outer join versions_per_customer v
  on v.customer_unique_id = e.customer_unique_id
where coalesce(e.changing_events, -1) <> coalesce(v.versions, -1)
