{{ config(materialized='view') }}

SELECT
    product_id,
    product_name,
    category_id,
    subcategory_id,
    brand,
    COALESCE(price, 0) AS price,
    COALESCE(cost, 0) AS cost,
    price - cost AS margin,
    CASE 
        WHEN price > 0 THEN (price - cost) / price 
        ELSE 0 
    END AS margin_percentage,
    weight,
    dimensions,
    color,
    size,
    is_active,
    created_at,
    updated_at
FROM {{ source('raw_ecommerce', 'products') }}
WHERE product_name IS NOT NULL
