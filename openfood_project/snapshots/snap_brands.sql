{% snapshot snap_brands %}
    {{
        config(
            target_schema='snapshots',
            unique_key=['product_id', 'brand_name'],
            strategy='check',
            check_cols=['brand_name']
        )
    }}
    
    select
        product_id,
        product_code,
        brand_name,
        loaded_at
    from {{ ref('stg_brands') }}
    
{% endsnapshot %}
