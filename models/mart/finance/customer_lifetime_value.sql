{{ config(materialized='table') }}

WITH customer_cohorts AS (
    SELECT 
        customer_id,
        DATE_TRUNC('month', registration_date) AS cohort_month,
        registration_date,
        first_order_date,
        last_order_date,
        total_orders,
        total_spent,
        customer_lifespan_days
    FROM {{ ref('int_customer_metrics') }}
    WHERE total_orders > 0
),

monthly_revenue AS (
    SELECT 
        cc.customer_id,
        cc.cohort_month,
        DATE_TRUNC('month', fo.order_date) AS revenue_month,
        DATEDIFF(DATE_TRUNC('month', fo.order_date), cc.cohort_month) / 30 AS period_number,
        SUM(fo.total_amount) AS monthly_revenue,
        COUNT(fo.order_id) AS monthly_orders
    FROM customer_cohorts cc
    JOIN {{ ref('fct_orders') }} fo
        ON cc.customer_id = fo.customer_id
    GROUP BY 1, 2, 3, 4
),

clv_calculation AS (
    SELECT 
        customer_id,
        cohort_month,
        SUM(monthly_revenue) AS total_clv,
        AVG(monthly_revenue) AS avg_monthly_revenue,
        COUNT(DISTINCT revenue_month) AS active_months,
        MAX(period_number) AS tenure_months,
        
        -- Predicted CLV (simple linear extrapolation)
        CASE 
            WHEN COUNT(DISTINCT revenue_month) >= 3 THEN
                SUM(monthly_revenue) + (AVG(monthly_revenue) * 6)  -- 6 month projection
            ELSE total_spent
        END AS predicted_clv_6m
        
    FROM monthly_revenue
    GROUP BY 1, 2
)

SELECT 
    clc.*,
    cm.customer_segment,
    cm.value_segment,
    cm.total_orders,
    cm.avg_order_value,
    
    -- CLV metrics
    total_clv / NULLIF(total_orders, 0) AS revenue_per_order,
    total_clv / NULLIF(tenure_months, 0) AS revenue_per_month,
    
    -- Cohort analysis
    ROW_NUMBER() OVER (
        PARTITION BY cohort_month 
        ORDER BY total_clv DESC
    ) AS clv_rank_in_cohort
    
FROM clv_calculation clc
JOIN {{ ref('int_customer_metrics') }} cm
    ON clc.customer_id = cm.customer_id
