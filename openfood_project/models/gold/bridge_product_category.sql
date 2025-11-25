{{ config(
    materialized='table',
    schema='gold'
) }}

-- Bridge table: product to category (many-to-many)
select distinct
    product_id,
    category_name
from {{ ref('stg_categories') }}
