-- Custom macro for revenue calculations
{% macro calculate_revenue_metrics(table_name, date_column, revenue_column) %}
    SELECT 
        {{ date_column }},
        SUM({{ revenue_column }}) AS total_revenue,
        COUNT(*) AS total_transactions,
        AVG({{ revenue_column }}) AS avg_transaction_value,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {{ revenue_column }}) AS median_revenue,
        STDDEV({{ revenue_column }}) AS revenue_stddev
    FROM {{ table_name }}
    GROUP BY {{ date_column }}
{% endmacro %}

-- Macro for data freshness checks
{% macro freshness_check(table_name, date_column, max_days=1) %}
    SELECT 
        '{{ table_name }}' AS table_name,
        MAX({{ date_column }}) AS last_update,
        DATEDIFF(CURRENT_DATE(), MAX({{ date_column }})) AS days_old,
        CASE 
            WHEN DATEDIFF(CURRENT_DATE(), MAX({{ date_column }})) > {{ max_days }}
            THEN 'STALE'
            ELSE 'FRESH'
        END AS freshness_status
    FROM {{ table_name }}
{% endmacro %}
