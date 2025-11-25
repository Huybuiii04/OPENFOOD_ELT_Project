{{ config(
    materialized='table',
    schema='gold'
) }}

-- Bridge table: product to brand (many-to-many)
select distinct
    product_id,
    brand_name
from {{ ref('stg_brands') }}
