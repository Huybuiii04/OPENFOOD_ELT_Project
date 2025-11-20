{{ config(
    materialized='table',
    schema='silver'
) }}

-- Helper function to remove accents and normalize Unicode
with raw_products as (
    select
        id,
        code,
        product_name,
        brands,
        countries,
        categories,
        ingredients_text,
        nutriscore_grade,
        energy_100g,
        sugars_100g,
        current_timestamp() as loaded_at
    from {{ source('raw', 'products') }}
    where product_name is not null and trim(product_name) != ''
),

cleaned_products as (
    select
        -- ID
        coalesce(trim(id), '') as id,
        
        -- Code: trim + lowercase
        coalesce(trim(lower(code)), '') as code,
        
        -- Product Name: trim + remove accents + normalize spaces
        coalesce(
            trim(
                regexp_replace(
                    regexp_replace(
                        -- Remove diacritics (accents)
                        translate(
                            product_name,
                            'áàảãạăắằẳẵặâấầẩẫậéèẻẽẹêếềểễệíìỉĩịóòỏõọôốồổỗộơớờởỡợúùủũụưứừửữựýỳỷỹỵđ',
                            'aaaaaaaaaaaaaaaaaeeeeeeeeeeiiiiiooooooooooooooouuuuuuuuuuuyyyyyd'
                        ),
                        -- Remove special characters (keep alphanumeric, spaces, hyphens)
                        '[^a-zA-Z0-9\s\-]',
                        ''
                    ),
                    -- Normalize multiple spaces to single space
                    '\s+',
                    ' '
                )
            ),
            ''
        ) as product_name,
        
        -- Brands: trim + lowercase + remove accents
        coalesce(
            trim(
                regexp_replace(
                    translate(
                        lower(brands),
                        'áàảãạăắằẳẵặâấầẩẫậéèẻẽẹêếềểễệíìỉĩịóòỏõọôốồổỗộơớờởỡợúùủũụưứừửữựýỳỷỹỵđ',
                        'aaaaaaaaaaaaaaaaaeeeeeeeeeeiiiiiooooooooooooooouuuuuuuuuuuyyyyyd'
                    ),
                    '[^a-z0-9,\s\-]',
                    ''
                )
            ),
            ''
        ) as brands,
        
        -- Countries: trim + uppercase
        coalesce(
            trim(
                regexp_replace(
                    upper(countries),
                    '[^A-Z0-9,\s\-]',
                    ''
                )
            ),
            ''
        ) as countries,
        
        -- Categories: trim + lowercase + remove accents
        coalesce(
            trim(
                regexp_replace(
                    translate(
                        lower(categories),
                        'áàảãạăắằẳẵặâấầẩẫậéèẻẽẹêếềểễệíìỉĩịóòỏõọôốồổỗộơớờởỡợúùủũụưứừửữựýỳỷỹỵđ',
                        'aaaaaaaaaaaaaaaaaeeeeeeeeeeiiiiiooooooooooooooouuuuuuuuuuuyyyyyd'
                    ),
                    '[^a-z0-9,\s\-]',
                    ''
                )
            ),
            ''
        ) as categories,
        
        -- Ingredients: trim + lowercase + remove accents
        coalesce(
            trim(
                regexp_replace(
                    translate(
                        lower(ingredients_text),
                        'áàảãạăắằẳẵặâấầẩẫậéèẻẽẹêếềểễệíìỉĩịóòỏõọôốồổỗộơớờởỡợúùủũụưứừửữựýỳỷỹỵđ',
                        'aaaaaaaaaaaaaaaaaeeeeeeeeeeiiiiiooooooooooooooouuuuuuuuuuuyyyyyd'
                    ),
                    '\s+',
                    ' '
                )
            ),
            ''
        ) as ingredients_text,
        
        -- Nutriscore: uppercase + trim + remove invalid chars
        case
            when trim(upper(nutriscore_grade)) in ('A', 'B', 'C', 'D', 'E') 
            then trim(upper(nutriscore_grade))
            else null
        end as nutriscore_grade,
        
        -- Energy: handle NULL and negative values
        case
            when energy_100g is not null and energy_100g > 0 then energy_100g
            when energy_100g is null or energy_100g <= 0 then null
            else energy_100g
        end as energy_100g,
        
        -- Sugars: handle NULL and negative values
        case
            when sugars_100g is not null and sugars_100g >= 0 then sugars_100g
            when sugars_100g is null or sugars_100g < 0 then null
            else sugars_100g
        end as sugars_100g,
        
        loaded_at
    from raw_products
)

select
    id,
    code,
    product_name,
    brands,
    countries,
    categories,
    ingredients_text,
    nutriscore_grade,
    energy_100g,
    sugars_100g,
    loaded_at
from cleaned_products
where trim(product_name) != ''  -- Final filter to exclude empty product names

