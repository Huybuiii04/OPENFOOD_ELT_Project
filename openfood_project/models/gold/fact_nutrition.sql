{{ config(
    materialized='table',
    schema='gold'
) }}

-- Star Schema: fact_nutrition joins with dimensions only
with fact_base as (
    select
        row_number() over (order by p.product_sk) as fact_id,
        p.product_sk,
        b.brand_sk,
        c.category_sk,
        co.country_sk,
        sp.energy_100g,
        sp.sugars_100g,
        sp.nutriscore_grade,
        sp.loaded_at as load_time
    from {{ ref('stg_products') }} sp
    inner join {{ ref('dim_product') }} p 
        on sp.id = p.product_id
        and p.is_current = true
    left join {{ ref('stg_brands') }} sb 
        on sp.id = sb.product_id
    left join {{ ref('dim_brand') }} b 
        on sb.brand_name = b.brand_name
        and b.is_current = true
    left join {{ ref('stg_categories') }} sc 
        on sp.id = sc.product_id
    left join {{ ref('dim_category') }} c 
        on sc.category_name = c.category_name
        and c.is_current = true
    left join {{ ref('stg_countries') }} sco 
        on sp.id = sco.product_id
    left join {{ ref('dim_country') }} co 
        on sco.country_code = co.country_code
        and co.is_current = true
    qualify row_number() over (partition by p.product_sk, b.brand_sk, c.category_sk, co.country_sk order by sp.loaded_at desc) = 1
)

select * from fact_base #fact_nutrition
