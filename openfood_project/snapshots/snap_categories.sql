{% snapshot snap_categories %}
    {{
        config(
            target_schema='snapshots',
            unique_key='category_name',
            strategy='check',
            check_cols=['category_name']
        )
    }}
    
    select distinct
        category_name,
        current_timestamp() as loaded_at
    from {{ ref('stg_categories') }}
    where category_name is not null and trim(category_name) != ''
    
{% endsnapshot %}
