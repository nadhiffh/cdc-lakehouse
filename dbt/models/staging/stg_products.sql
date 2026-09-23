-- Products. No update events in this source, so latest-per-key is the row.
-- 610 of 32,951 have no category; kept as NULL and surfaced as 'unknown' in
-- the mart rather than dropped.

with latest as (

    select
        pk,
        after,
        row_number() over (
            partition by pk
            order by coalesce(lsn, 0) desc, kafka_offset desc
        ) as rn
    from {{ source('landing', 'cdc_events') }}
    where source_table = 'product'
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

from latest
where rn = 1
