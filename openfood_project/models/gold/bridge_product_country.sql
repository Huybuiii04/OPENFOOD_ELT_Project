{{ config(
    materialized='table',
    schema='gold'
) }}

-- Bridge table: product to country (many-to-many)
select distinct
    product_id,
    country_code
from {{ ref('stg_countries') }}
