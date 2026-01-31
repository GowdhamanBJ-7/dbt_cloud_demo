{{ config(materialized='view') }}

WITH order_details AS (
    SELECT 
        o.*,
        c.email AS customer_email,
        c.country_code,
        c.registration_date,
        DATEDIFF(o.order_date, c.registration_date) AS days_since_registration,
        
        -- Time-based features
        DAYOFWEEK(o.order_date) AS order_day_of_week,
        MONTH(o.order_date) AS order_month,
        YEAR(o.order_date) AS order_year,
        QUARTER(o.order_date) AS order_quarter,
        
        -- Order categorization
        CASE 
            WHEN o.total_amount < 50 THEN 'Small'
            WHEN o.total_amount BETWEEN 50 AND 200 THEN 'Medium'
            WHEN o.total_amount > 200 THEN 'Large'
        END AS order_size_category
        
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_customers') }} c
        ON o.customer_id = c.customer_id
),

order_items_agg AS (
    SELECT 
        oi.order_id,
        COUNT(*) AS total_items,
        COUNT(DISTINCT oi.product_id) AS unique_products,
        SUM(oi.quantity) AS total_quantity,
        AVG(p.price) AS avg_product_price,
        SUM(p.margin * oi.quantity) AS total_margin
    FROM {{ ref("stg_order_items") }} oi
    JOIN {{ ref('stg_products') }} p
        ON oi.product_id = p.product_id
    GROUP BY 1
)

SELECT 
    od.*,
    oia.total_items,
    oia.unique_products,
    oia.total_quantity,
    oia.avg_product_price,
    oia.total_margin,
    COALESCE(oia.total_margin / NULLIF(od.total_amount, 0), 0) AS margin_percentage
FROM order_details od
LEFT JOIN order_items_agg oia
    ON od.order_id = oia.order_id
