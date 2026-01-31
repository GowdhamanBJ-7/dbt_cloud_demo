{{ config(
    materialized='incremental',
    unique_key='revenue_month',
    on_schema_change='fail'
) }}

WITH monthly_metrics AS (
    SELECT
        DATE_TRUNC('month', order_date) AS revenue_month,
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT customer_id) AS unique_customers,
        SUM(total_amount) AS gross_revenue,
        SUM(tax_amount) AS total_tax,
        SUM(shipping_amount) AS total_shipping,
        SUM(discount_amount) AS total_discounts,
        SUM(total_margin) AS total_margin,
        AVG(total_amount) AS avg_order_value,
        
        -- Growth calculations
        LAG(SUM(total_amount)) OVER (ORDER BY DATE_TRUNC('month', order_date)) AS prev_month_revenue,
        LAG(COUNT(DISTINCT order_id)) OVER (ORDER BY DATE_TRUNC('month', order_date)) AS prev_month_orders
        
    FROM {{ ref('fct_orders') }}
    WHERE is_successful_order = TRUE
    
    {% if is_incremental() %}
        AND order_date >= (SELECT DATEADD('month', -1, MAX(revenue_month)) FROM {{ this }})
    {% endif %}
    
    GROUP BY 1
),

growth_metrics AS (
    SELECT
        *,
        CASE 
            WHEN prev_month_revenue IS NOT NULL AND prev_month_revenue > 0
            THEN (gross_revenue - prev_month_revenue) / prev_month_revenue * 100
            ELSE NULL
        END AS revenue_growth_pct,
        
        CASE 
            WHEN prev_month_orders IS NOT NULL AND prev_month_orders > 0
            THEN (total_orders - prev_month_orders) / prev_month_orders * 100
            ELSE NULL
        END AS order_growth_pct,
        
        total_margin / NULLIF(gross_revenue, 0) * 100 AS margin_percentage
        
    FROM monthly_metrics
)

SELECT
    revenue_month,
    total_orders,
    unique_customers,
    gross_revenue,
    total_tax,
    total_shipping,
    total_discounts,
    gross_revenue - total_tax - total_shipping AS net_revenue,
    total_margin,
    margin_percentage,
    avg_order_value,
    revenue_growth_pct,
    order_growth_pct,
    CURRENT_TIMESTAMP AS dbt_updated_at
FROM growth_metrics
