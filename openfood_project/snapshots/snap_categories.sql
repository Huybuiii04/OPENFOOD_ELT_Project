{% snapshot snap_categories %}
    {{
        config(
            target_schema='snapshots',
            unique_key=['product_id', 'category_name'],
            strategy='check',
            check_cols=['category_name']
        )
    }}
    
    select
        product_id,
        product_code,
        category_name,
        loaded_at
    from {{ ref('stg_categories') }}
    
{% endsnapshot %}
