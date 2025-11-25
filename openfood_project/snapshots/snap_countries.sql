{% snapshot snap_countries %}
    {{
        config(
            target_schema='snapshots',
            unique_key='country_code',
            strategy='check',
            check_cols=['country_code']
        )
    }}
    
    select distinct
        country_code,
        current_timestamp() as loaded_at
    from {{ ref('stg_countries') }}
    where country_code is not null and trim(country_code) != ''
    
{% endsnapshot %}
