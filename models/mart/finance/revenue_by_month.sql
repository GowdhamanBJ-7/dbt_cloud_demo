{{ config(
    materialized = 'incremental',
    unique_key = 'revenue_month',
    on_schema_change = 'fail'
) }}

WITH monthly_metrics AS (

    SELECT
        DATE_TRUNC('month', order_date) AS revenue_month,

        -- Order metrics
        COUNT(DISTINCT order_id) AS total_orders,
        COUNT(DISTINCT customer_id) AS unique_customers,

        -- Revenue metrics
        SUM(total_amount) AS gross_revenue,
        SUM(tax_amount) AS total_tax,
        SUM(shipping_amount) AS total_shipping,
        SUM(discount_amount) AS total_discounts,
        SUM(total_margin) AS total_margin,

        -- Averages
        AVG(total_amount) AS avg_order_value

    FROM {{ ref('fct_orders') }}
    WHERE is_successful_order = TRUE

    {% if is_incremental() %}
        -- Reprocess last 2 months to handle late-arriving data
        AND order_date >= (
            SELECT DATEADD('month', -2, MAX(revenue_month))
            FROM {{ this }}
        )
    {% endif %}

    GROUP BY 1
),

growth_metrics AS (

    SELECT
        *,

        -- Previous period metrics
        LAG(gross_revenue) OVER (ORDER BY revenue_month) AS prev_month_revenue,
        LAG(total_orders) OVER (ORDER BY revenue_month) AS prev_month_orders,

        -- Growth %
        CASE
            WHEN LAG(gross_revenue) OVER (ORDER BY revenue_month) > 0
            THEN
                (gross_revenue - LAG(gross_revenue) OVER (ORDER BY revenue_month))
                / LAG(gross_revenue) OVER (ORDER BY revenue_month) * 100
            ELSE NULL
        END AS revenue_growth_pct,

        CASE
            WHEN LAG(total_orders) OVER (ORDER BY revenue_month) > 0
            THEN
                (total_orders - LAG(total_orders) OVER (ORDER BY revenue_month))
                / LAG(total_orders) OVER (ORDER BY revenue_month) * 100
            ELSE NULL
        END AS order_growth_pct,

        -- Profitability
        total_margin / NULLIF(gross_revenue, 0) * 100 AS margin_percentage

    FROM monthly_metrics
)

SELECT
    revenue_month,

    -- Volume
    total_orders,
    unique_customers,

    -- Revenue
    gross_revenue,
    total_tax,
    total_shipping,
    total_discounts,
    gross_revenue - total_tax - total_shipping AS net_revenue,

    -- Profitability
    total_margin,
    margin_percentage,

    -- Averages
    avg_order_value,

    -- Growth
    revenue_growth_pct,
    order_growth_pct,

    -- Audit
    CURRENT_TIMESTAMP AS dbt_updated_at

FROM growth_metrics
