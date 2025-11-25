{{ config(
    materialized='table',
    schema='gold'
) }}

-- Star Schema: FACT joins only DIM tables via bridge tables
with fact_base as (
    select
        id as product_id,
        energy_100g,
        sugars_100g,
        nutriscore_grade,
        loaded_at
    from {{ ref('stg_products') }}
),

joined as (
    select
        row_number() over (order by p.product_sk, b.brand_sk, c.category_sk, co.country_sk) as fact_id,
        p.product_sk,
        b.brand_sk,
        c.category_sk,
        co.country_sk,
        fb.energy_100g,
        fb.sugars_100g,
        fb.nutriscore_grade,
        fb.loaded_at as load_time
    from fact_base fb
    left join {{ ref('dim_product') }} p 
        on fb.product_id = p.product_id 
        and p.is_current = true
    left join {{ ref('bridge_product_brand') }} pb 
        on fb.product_id = pb.product_id
    left join {{ ref('dim_brand') }} b 
        on pb.brand_name = b.brand_name 
        and b.is_current = true
    left join {{ ref('bridge_product_category') }} pc 
        on fb.product_id = pc.product_id
    left join {{ ref('dim_category') }} c 
        on pc.category_name = c.category_name 
        and c.is_current = true
    left join {{ ref('bridge_product_country') }} pco 
        on fb.product_id = pco.product_id
    left join {{ ref('dim_country') }} co 
        on pco.country_code = co.country_code 
        and co.is_current = true
    qualify row_number() over (partition by p.product_sk, b.brand_sk, c.category_sk, co.country_sk order by fb.loaded_at desc) = 1
)

select * from joined
