{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_ecommerce', 'order_items') }}
),

cleaned AS (
    SELECT
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        COALESCE(discount_amount, 0) AS discount_amount,
        quantity * unit_price - COALESCE(discount_amount, 0) AS line_total,
        created_at,
        updated_at
    FROM source_data
    WHERE quantity > 0
      AND unit_price >= 0
)

SELECT * FROM cleaned
