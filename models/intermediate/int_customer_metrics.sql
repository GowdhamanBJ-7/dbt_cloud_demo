-- Purpose: Business logic + aggregations
-- How valuable is each customer?

{{ config(materialized='view') }}

WITH customer_orders AS (
    SELECT 
        c.customer_id,
        c.email,
        c.registration_date,
        c.country_code,
        COUNT(o.order_id) AS total_orders,
        COALESCE(SUM(o.total_amount), 0) AS total_spent,
        COALESCE(AVG(o.total_amount), 0) AS avg_order_value,
        MIN(o.order_date) AS first_order_date,
        MAX(o.order_date) AS last_order_date,
        DATEDIFF(MAX(o.order_date), MIN(o.order_date)) AS customer_lifespan_days
    FROM {{ ref('stg_customers') }} c
    LEFT JOIN {{ ref('stg_orders') }} o 
        ON c.customer_id = o.customer_id
        AND o.is_successful_order = TRUE
        {% if var('include_test_orders') == false %}
        AND o.is_test_order = FALSE
        {% endif %}
    GROUP BY 1, 2, 3, 4
),

customer_segments AS (
    SELECT 
        *,
        CASE 
            WHEN total_orders = 0 THEN 'New'
            WHEN total_orders = 1 THEN 'One-time'
            WHEN total_orders BETWEEN 2 AND 5 THEN 'Regular'
            WHEN total_orders > 5 THEN 'VIP'
        END AS customer_segment,
        CASE 
            WHEN total_spent = 0 THEN 'No Purchase'
            WHEN total_spent < 100 THEN 'Low Value'
            WHEN total_spent BETWEEN 100 AND 500 THEN 'Medium Value'
            WHEN total_spent > 500 THEN 'High Value'
        END AS value_segment
    FROM customer_orders
)

SELECT * FROM customer_segments

-- Output (1 row per customer):
-- totalorders
-- totalspent
-- avgordervalue
-- customersegment
-- valuesegment