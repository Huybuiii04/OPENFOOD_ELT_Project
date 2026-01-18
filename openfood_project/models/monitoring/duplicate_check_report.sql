{{ config(
    materialized='view',
    schema='monitoring'
) }}

-- Comprehensive duplicate check report
WITH raw_duplicates AS (
    SELECT 
        'RAW - By ID' as check_type,
        id as key_value,
        COUNT(*) as occurrence_count,
        COUNT(*) - 1 as duplicate_count
    FROM {{ source('raw', 'products') }}
    GROUP BY id
    HAVING COUNT(*) > 1
),

raw_code_duplicates AS (
    SELECT 
        'RAW - By Code' as check_type,
        code as key_value,
        COUNT(*) as occurrence_count,
        COUNT(*) - 1 as duplicate_count
    FROM {{ source('raw', 'products') }}
    WHERE code IS NOT NULL AND code != ''
    GROUP BY code
    HAVING COUNT(*) > 1
),

silver_duplicates AS (
    SELECT 
        'SILVER - By ID' as check_type,
        id as key_value,
        COUNT(*) as occurrence_count,
        COUNT(*) - 1 as duplicate_count
    FROM {{ ref('stg_products') }}
    GROUP BY id
    HAVING COUNT(*) > 1
),

gold_duplicates AS (
    SELECT 
        'GOLD - By Product SK' as check_type,
        product_sk as key_value,
        COUNT(*) as occurrence_count,
        COUNT(*) - 1 as duplicate_count
    FROM {{ ref('dim_product') }}
    WHERE is_current = TRUE
    GROUP BY product_sk
    HAVING COUNT(*) > 1
)

SELECT * FROM raw_duplicates
UNION ALL
SELECT * FROM raw_code_duplicates
UNION ALL
SELECT * FROM silver_duplicates
UNION ALL
SELECT * FROM gold_duplicates
ORDER BY duplicate_count DESC
