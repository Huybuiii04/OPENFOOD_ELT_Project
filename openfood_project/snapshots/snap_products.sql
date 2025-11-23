{% snapshot snap_products %}
    {{
        config(
            target_schema='snapshots',
            unique_key='id',
            strategy='check',
            check_cols=['code', 'product_name', 'ingredients_text', 'nutriscore_grade', 'energy_100g', 'sugars_100g']
        )
    }}
    
    with dedup as (
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
            loaded_at,
            row_number() over (partition by id order by loaded_at desc) as rn
        from {{ ref('stg_products') }}
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
    from dedup
    where rn = 1
    
{% endsnapshot %}
