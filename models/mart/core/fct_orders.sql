{{ config(
    materialized='incremental',
    unique_key='order_id',
    on_schema_change='fail'
) }}

SELECT 
    order_id,
    customer_id,
    customer_email,
    order_date,
    status,
    total_amount,
    tax_amount,
    shipping_amount,
    discount_amount,
    total_items,
    unique_products,
    total_quantity,
    total_margin,
    margin_percentage,
    order_size_category,
    days_since_registration,
    order_day_of_week,
    order_month,
    order_quarter,
    order_year,
    country_code,
    payment_method,
    is_successful_order,
    created_at,
    updated_at
FROM {{ ref('int_order_enriched') }}
WHERE is_successful_order = TRUE

{% if is_incremental() %}
    AND updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
