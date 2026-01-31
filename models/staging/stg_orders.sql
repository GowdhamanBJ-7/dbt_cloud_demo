{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_ecommerce', 'orders') }}
),

cleaned AS (
    SELECT
        order_id,
        customer_id,
        order_date,
        status,
        COALESCE(total_amount, 0) AS total_amount,
        COALESCE(tax_amount, 0) AS tax_amount,
        COALESCE(shipping_amount, 0) AS shipping_amount,
        COALESCE(discount_amount, 0) AS discount_amount,
        payment_method,
        shipping_address,
        billing_address,
        coupon_code,
        created_at,
        updated_at,
        -- Business logic flags
        CASE 
            WHEN total_amount = 0 THEN TRUE 
            ELSE FALSE 
        END AS is_test_order,
        CASE 
            WHEN status IN ('completed', 'shipped', 'delivered') THEN TRUE 
            ELSE FALSE 
        END AS is_successful_order
    FROM source_data
    WHERE order_date IS NOT NULL
)

SELECT * FROM cleaned
