{{ config(
    materialized='table',
    schema='gold'
) }}

with stg_categories as (
    select
        category_id,
        product_id,
        category_name,
        loaded_at
    from {{ ref('stg_categories') }}
),

with_hash as (
    select
        category_id,
        category_name,
        md5(concat(category_name)) as category_hash,
        loaded_at,
        row_number() over (partition by category_name order by loaded_at desc) as rn
    from stg_categories
),

scd_type_2 as (
    select
        {{ dbt_utils.generate_surrogate_key(['category_name']) }} as category_sk,
        category_name,
        category_hash,
        loaded_at as valid_from,
        null::timestamp as valid_to,
        true as is_current
    from with_hash
    where rn = 1
)

select
    category_sk,
    category_name,
    valid_from,
    valid_to,
    is_current
from scd_type_2
