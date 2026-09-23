-- Sellers. Profiling found zero attribute drift across all 3,095 sellers
-- (no duplicate ids, no city or zip variation), so this stays a plain SCD
-- Type 1 dimension. Applying SCD2 here would add history columns that could
-- never hold more than one version per key.

with latest as (

    select
        pk,
        after,
        row_number() over (
            partition by pk
            order by coalesce(lsn, 0) desc, kafka_offset desc
        ) as rn
    from {{ source('landing', 'cdc_events') }}
    where source_table = 'seller'
      and op <> 'delete'

)

select
    json_extract_string(after, '$.seller_id')       as seller_id,
    json_extract_string(after, '$.zip_code_prefix') as zip_code_prefix,
    json_extract_string(after, '$.city')            as city,
    json_extract_string(after, '$.state')           as state

from latest
where rn = 1
