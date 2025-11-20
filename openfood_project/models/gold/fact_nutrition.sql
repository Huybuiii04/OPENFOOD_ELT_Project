{{ config(
    materialized='table',
    schema='gold'
) }}

with stg_products as (
    select
        id as product_id,
        code,
        energy_100g,
        sugars_100g,
        loaded_at
    from {{ ref('stg_products') }}
),

stg_brands as (
    select distinct
        product_id,
        brand_id,
        brand_name
    from {{ ref('stg_brands') }}
),

stg_categories as (
    select distinct
        product_id,
        category_id,
        category_name
    from {{ ref('stg_categories') }}
),

stg_countries as (
    select distinct
        product_id,
        country_id,
        country_code
    from {{ ref('stg_countries') }}
),

dim_product as (
    select
        product_sk,
        product_id,
        code
    from {{ ref('dim_product') }}
    where is_current = true
),

dim_brand as (
    select
        brand_sk,
        brand_name
    from {{ ref('dim_brand') }}
    where is_current = true
),

dim_category as (
    select
        category_sk,
        category_name
    from {{ ref('dim_category') }}
    where is_current = true
),

dim_country as (
    select
        country_sk,
        country_code
    from {{ ref('dim_country') }}
    where is_current = true
),

-- Join all dimensions
fact_nutrition as (
    select
        row_number() over (order by p.product_id, b.brand_sk, c.category_sk, co.country_sk) as fact_id,
        p.product_sk,
        b.brand_sk,
        c.category_sk,
        co.country_sk,
        sp.energy_100g,
        sp.sugars_100g,
        sp.loaded_at as load_time
    from stg_products sp
    left join stg_brands sb on sp.product_id = sb.product_id
    left join dim_brand b on sb.brand_name = b.brand_name
    left join stg_categories sc on sp.product_id = sc.product_id
    left join dim_category c on sc.category_name = c.category_name
    left join stg_countries sco on sp.product_id = sco.product_id
    left join dim_country co on sco.country_code = co.country_code
    left join dim_product p on sp.product_id = p.product_id
)

select
    fact_id,
    product_sk,
    brand_sk,
    category_sk,
    country_sk,
    energy_100g,
    sugars_100g,
    load_time
from fact_nutrition
