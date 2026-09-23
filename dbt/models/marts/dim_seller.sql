{{ config(materialized='table') }}

-- SCD Type 1. Profiling found zero attribute drift across all 3,095 sellers,
-- so there is no history to keep. Modelling this as SCD2 would produce a
-- dimension where every key has exactly one version, which is more misleading
-- than useful: it would imply history is tracked when none exists.

select
    {{ dbt_utils.generate_surrogate_key(['seller_id']) }} as seller_key,
    seller_id,
    zip_code_prefix,
    city,
    state
from {{ ref('stg_sellers') }}
