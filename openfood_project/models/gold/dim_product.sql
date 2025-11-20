{{ config(
    materialized='table',
    schema='gold'
) }}

with stg_products as (
    select
        id,
        code,
        product_name,
        ingredients_text,
        nutriscore_grade,
        loaded_at
    from {{ ref('stg_products') }}
),

-- Use hash to detect changes
with_hash as (
    select
        id,
        code,
        product_name,
        ingredients_text,
        nutriscore_grade,
        md5(concat(code, '|', product_name, '|', ingredients_text, '|', nutriscore_grade)) as product_hash,
        loaded_at
    from stg_products
),

-- Detect changes and apply SCD Type 2
scd_type_2 as (
    select
        {{ dbt_utils.generate_surrogate_key(['id']) }} as product_sk,
        id as product_id,
        code,
        product_name,
        ingredients_text,
        nutriscore_grade,
        product_hash,
        loaded_at as valid_from,
        null::timestamp as valid_to,
        true as is_current,
        row_number() over (partition by id order by loaded_at desc) as rn
    from with_hash
)

select
    product_sk,
    product_id,
    code,
    product_name,
    ingredients_text,
    nutriscore_grade,
    valid_from,
    valid_to,
    is_current
from scd_type_2
where rn = 1
