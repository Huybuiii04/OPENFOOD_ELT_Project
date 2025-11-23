{{ config(
    materialized='table',
    schema='gold'
) }}

-- Use dbt snapshot for automatic SCD Type 2 tracking
select
    {{ dbt_utils.generate_surrogate_key(['category_name']) }} as category_sk,
    category_name,
    dbt_valid_from as valid_from,
    dbt_valid_to as valid_to,
    case when dbt_valid_to is null then true else false end as is_current
from {{ ref('snap_categories') }}
