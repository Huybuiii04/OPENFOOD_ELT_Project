{{ config(
    materialized='table',
    schema='silver'
) }}

with products as (
    select 
        id as product_id,
        code as product_code,
        product_name,
        categories
    from {{ ref('stg_products') }}
    where categories is not null and trim(categories) != ''
)

select
    {{ dbt_utils.generate_surrogate_key(['product_id', 'category_name']) }} as category_id,
    product_id,
    product_code,
    trim(category_name) as category_name,
    current_timestamp() as loaded_at
from products,
lateral flatten(input => split(categories, ',')) as f(category_name)
where trim(category_name) != ''
