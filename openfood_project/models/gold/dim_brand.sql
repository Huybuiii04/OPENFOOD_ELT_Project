{{ config(
    materialized='table',
    schema='gold'
) }}

with stg_brands as (
    select
        brand_id,
        product_id,
        brand_name,
        loaded_at
    from {{ ref('stg_brands') }}
),

with_hash as (
    select
        brand_id,
        brand_name,
        md5(concat(brand_name)) as brand_hash,
        loaded_at,
        row_number() over (partition by brand_name order by loaded_at desc) as rn
    from stg_brands
),

scd_type_2 as (
    select
        {{ dbt_utils.generate_surrogate_key(['brand_name']) }} as brand_sk,
        brand_name,
        brand_hash,
        loaded_at as valid_from,
        null::timestamp as valid_to,
        true as is_current
    from with_hash
    where rn = 1
)

select
    brand_sk,
    brand_name,
    valid_from,
    valid_to,
    is_current
from scd_type_2
