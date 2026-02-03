{{ freshness_check(
    table_name = ref('fct_orders'),
    date_column = "updated_at",
    max_days = 1
) }}

