{{ config(
    materialized='table',
    schema='silver'
) }}

with products as (
    select 
        id as product_id,
        code as product_code,
        product_name,
        countries
    from {{ ref('stg_products') }}
    where countries is not null and trim(countries) != ''
)

select
    {{ dbt_utils.generate_surrogate_key(['product_id', 'country_code']) }} as country_id,
    product_id,
    product_code,
    trim(country_code) as country_code,
    current_timestamp() as loaded_at
from products,
lateral flatten(input => split(countries, ',')) as f(country_code)
where trim(country_code) != ''
