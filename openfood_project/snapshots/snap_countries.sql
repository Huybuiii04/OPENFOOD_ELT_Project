{% snapshot snap_countries %}
    {{
        config(
            target_schema='snapshots',
            unique_key=['product_id', 'country_code'],
            strategy='check',
            check_cols=['country_code']
        )
    }}
    
    select
        product_id,
        product_code,
        country_code,
        loaded_at
    from {{ ref('stg_countries') }}
    
{% endsnapshot %}
