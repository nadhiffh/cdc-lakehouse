-- Sellers, collapsed to current state: the latest event per seller_id.
--
-- Profiling found zero attribute drift across all 3,095 sellers (no duplicate
-- ids, no city or zip variation), so this stays SCD Type 1. Applying SCD2 here
-- would add history columns that could never hold more than one version.
--
-- The delete filter is applied AFTER ranking. See stg_products for why: doing it
-- before would let a deleted key fall back to an older insert and reappear.

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
    where source_table = 'seller'

),

current_state as (

    select * from ranked
    where rn = 1
      and op <> 'delete'

)

select
    json_extract_string(after, '$.seller_id')       as seller_id,
    json_extract_string(after, '$.zip_code_prefix') as zip_code_prefix,
    json_extract_string(after, '$.city')            as city,
    json_extract_string(after, '$.state')           as state

from current_state
