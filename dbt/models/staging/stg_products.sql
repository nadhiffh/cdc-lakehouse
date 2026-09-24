-- Products, collapsed to current state: the latest event per product_id.
--
-- 610 of 32,951 have no category; kept as NULL here and surfaced as 'unknown'
-- in the mart rather than dropped.
--
-- The delete filter is applied AFTER ranking, not before. Filtering deletes out
-- of the candidate set first was a real bug: a row whose latest event is a
-- delete would fall back to its earlier insert and reappear in the mart as
-- though it were still live. Ranking every event and then discarding keys whose
-- newest event is a delete is the only order that handles resurrection
-- correctly, because a key can legitimately be inserted, deleted, and inserted
-- again.

with ranked as (

    select
        pk,
        op,
        after,
        row_number() over (
            partition by pk
            order by coalesce(lsn, 0) desc, kafka_offset desc
        ) as rn
    from {{ source('landing', 'cdc_events') }}
    where source_table = 'product'

),

current_state as (

    select * from ranked
    where rn = 1
      -- Dropped only when the most recent event for this key is a delete.
      and op <> 'delete'

)

select
    json_extract_string(after, '$.product_id')       as product_id,
    json_extract_string(after, '$.category_name')    as category_name,
    json_extract_string(after, '$.category_name_en') as category_name_en,
    cast(json_extract(after, '$.weight_g')  as integer) as weight_g,
    cast(json_extract(after, '$.length_cm') as integer) as length_cm,
    cast(json_extract(after, '$.height_cm') as integer) as height_cm,
    cast(json_extract(after, '$.width_cm')  as integer) as width_cm

from current_state
