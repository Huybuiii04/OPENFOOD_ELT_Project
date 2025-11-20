{{ config(
    materialized='table',
    schema='silver'
) }}

with products as (
    select 
        id as product_id,
        code as product_code,
        product_name,
        brands
    from {{ ref('stg_products') }}
    where brands is not null and trim(brands) != ''
)

select
    {{ dbt_utils.generate_surrogate_key(['product_id', 'brand_name']) }} as brand_id,
    product_id,
    product_code,
    trim(brand_name) as brand_name,
    current_timestamp() as loaded_at
from products,
lateral flatten(input => split(brands, ',')) as f(brand_name)
where trim(brand_name) != ''
