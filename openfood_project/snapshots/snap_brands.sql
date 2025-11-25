{% snapshot snap_brands %}
    {{
        config(
            target_schema='snapshots',
            unique_key='brand_name',
            strategy='check',
            check_cols=['brand_name']
        )
    }}
    
    select distinct
        brand_name,
        current_timestamp() as loaded_at
    from {{ ref('stg_brands') }}
    where brand_name is not null and trim(brand_name) != ''
    
{% endsnapshot %}
