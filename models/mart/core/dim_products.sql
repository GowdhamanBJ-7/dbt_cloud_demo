{{ config(materialized='table') }}

SELECT
    p.product_id,
    p.product_name,
    p.category_id,
    pc.category_name,
    pc.parent_category_id,
    p.subcategory_id,
    p.brand,
    p.price,
    p.cost,
    p.margin,
    p.margin_percentage,
    pc.margin_target,
    CASE 
        WHEN p.margin_percentage >= pc.margin_target THEN 'Above Target'
        WHEN p.margin_percentage >= pc.margin_target * 0.8 THEN 'Near Target'
        ELSE 'Below Target'
    END AS margin_performance,
    p.weight,
    p.dimensions,
    p.color,
    p.size,
    p.is_active,
    CURRENT_TIMESTAMP AS dbt_updated_at
    
FROM {{ ref('stg_products') }} p
LEFT JOIN {{ ref('product_categories') }} pc
    ON p.category_id = pc.category_id
