{{ config(materialized='table') }}

SELECT
    cm.customer_id,
    cm.email,
    cm.registration_date,
    cm.country_code,
    cm.customer_segment,
    cm.value_segment,
    cm.total_orders,
    cm.total_spent,
    cm.avg_order_value,
    cm.first_order_date,
    cm.last_order_date,
    cm.customer_lifespan_days,
    
    -- Customer Status
    CASE 
        WHEN cm.last_order_date >= CURRENT_DATE - INTERVAL 30 DAYS THEN 'Active'
        WHEN cm.last_order_date >= CURRENT_DATE - INTERVAL 90 DAYS THEN 'At Risk'
        WHEN cm.last_order_date >= CURRENT_DATE - INTERVAL 180 DAYS THEN 'Dormant'
        ELSE 'Churned'
    END AS customer_status,
    
    -- Recency, Frequency, Monetary (RFM) Analysis
    DATEDIFF(CURRENT_DATE, cm.last_order_date) AS recency_days,
    cm.total_orders AS frequency,
    cm.total_spent AS monetary_value,
    
    CURRENT_TIMESTAMP AS dbt_updated_at
    
FROM {{ ref('int_customer_metrics') }} cm
