-- Order line items. These never change after insert in this source (zero
-- update events across 112,650 rows), so the latest event per key is the row.

with latest as (

    select
        pk,
        after,
        row_number() over (
            partition by pk
            order by coalesce(lsn, 0) desc, kafka_offset desc
        ) as rn
    from {{ source('landing', 'cdc_events') }}
    where source_table = 'order_item'
      and op <> 'delete'

)

select
    json_extract_string(after, '$.order_id')                as order_id,
    cast(json_extract(after, '$.order_item_id') as integer) as order_item_id,
    json_extract_string(after, '$.product_id')              as product_id,
    json_extract_string(after, '$.seller_id')               as seller_id,

    -- decimal.handling.mode=string keeps money exact over the wire; casting
    -- here rather than letting it arrive as a float avoids drift on sums.
    cast(json_extract_string(after, '$.price')         as decimal(10, 2)) as price,
    cast(json_extract_string(after, '$.freight_value') as decimal(10, 2)) as freight_value,

    to_timestamp(cast(json_extract(after, '$.shipping_limit_at') as bigint) / 1000)
        as shipping_limit_at

from latest
where rn = 1
