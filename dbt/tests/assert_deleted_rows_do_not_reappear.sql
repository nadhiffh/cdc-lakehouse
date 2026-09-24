-- A key whose most recent CDC event is a delete must not appear in any
-- current-state model.
--
-- This caught a real bug. The staging models originally filtered
-- `op <> 'delete'` *before* ranking events, which removed the delete from the
-- candidate set and let the row fall back to its earlier insert. A deleted
-- product stayed in dim_product looking perfectly live, and every existing test
-- passed: uniqueness held, not-null held, referential integrity held. Only the
-- absolute row count was wrong, by one.
--
-- Resurrection is handled too: a key inserted, deleted, then inserted again is
-- legitimately current, so this checks the newest event per key rather than
-- whether a delete exists anywhere in its history.

with newest_event as (

    select
        source_table,
        pk,
        op
    from (
        select
            source_table,
            pk,
            op,
            row_number() over (
                partition by source_table, pk
                order by coalesce(lsn, 0) desc, kafka_offset desc
            ) as rn
        from {{ source('landing', 'cdc_events') }}
    )
    where rn = 1

),

deleted_keys as (

    select source_table, pk from newest_event where op = 'delete'

),

leaked as (

    select 'dim_product' as model, d.pk
    from deleted_keys d
    join {{ ref('dim_product') }} m on m.product_id = d.pk
    where d.source_table = 'product'

    union all

    select 'dim_seller' as model, d.pk
    from deleted_keys d
    join {{ ref('dim_seller') }} m on m.seller_id = d.pk
    where d.source_table = 'seller'

    union all

    select 'fct_orders' as model, d.pk
    from deleted_keys d
    join {{ ref('fct_orders') }} m on m.order_id = d.pk
    where d.source_table = 'order'

    union all

    select 'fct_order_items' as model, d.pk
    from deleted_keys d
    join {{ ref('fct_order_items') }} m
      on m.order_id || ':' || m.order_item_id = d.pk
    where d.source_table = 'order_item'

)

select * from leaked
