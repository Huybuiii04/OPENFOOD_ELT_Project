{{ config(
    materialized='table',
    schema='gold'
) }}

with stg_countries as (
    select
        country_id,
        product_id,
        country_code,
        loaded_at
    from {{ ref('stg_countries') }}
),

with_hash as (
    select
        country_id,
        country_code,
        md5(concat(country_code)) as country_hash,
        loaded_at,
        row_number() over (partition by country_code order by loaded_at desc) as rn
    from stg_countries
),

scd_type_2 as (
    select
        {{ dbt_utils.generate_surrogate_key(['country_code']) }} as country_sk,
        country_code,
        country_hash,
        loaded_at as valid_from,
        null::timestamp as valid_to,
        true as is_current
    from with_hash
    where rn = 1
)

select
    country_sk,
    country_code,
    valid_from,
    valid_to,
    is_current
from scd_type_2
