{% snapshot customer_snapshot %}
    {{
        config(
            target_schema='snapshots',
            unique_key='customer_id',
            strategy='timestamp',
            updated_at='updated_at',
        )
    }}
    
    SELECT 
        customer_id,
        email,
        first_name,
        last_name,
        country_code,
        is_active,
        updated_at
    FROM {{ source('raw_ecommerce', 'customers') }}
    
{% endsnapshot %}
