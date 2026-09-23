{{ config(materialized='table') }}

-- SCD Type 1, same reasoning as dim_seller: no product update events exist in
-- this source.
--
-- 610 of 32,951 products carry no category. They are surfaced as 'unknown'
-- rather than dropped, so line items always resolve to a product and the fact
-- table reconciles 1:1 with the source.

select
    {{ dbt_utils.generate_surrogate_key(['product_id']) }} as product_key,
    product_id,
    coalesce(category_name,    'unknown') as category_name,
    coalesce(category_name_en, 'unknown') as category_name_en,
    category_name is null                 as is_category_missing,
    weight_g,
    length_cm,
    height_cm,
    width_cm
from {{ ref('stg_products') }}
