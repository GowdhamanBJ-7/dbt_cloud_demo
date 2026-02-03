{{ config(materialized="view") }}
-- Purpose: Clean, standardize, rename, filter
with
    source_data as (select * from {{ source("raw_ecommerce", "customers") }}),

    cleaned as (
        select
            customer_id,
            trim(lower(email)) as email,
            trim(first_name) as first_name,
            trim(last_name) as last_name,
            CASE
            WHEN phone IS NOT NULL THEN
                REGEXP_REPLACE(phone, '[^0-9]', '')
            END AS phone_clean,
            registration_date,
            case
                when country_code in ('US', 'CA', 'UK', 'IND')
                then country_code
                else 'OTHER'
            end as country_code,
            date_of_birth,
            case
                when gender in ('M', 'F', 'Male', 'Female')
                then upper(left(gender, 1))
                else 'U'
            end as gender,
            is_active,
            created_at,
            updated_at
        from source_data
        where email is not null and registration_date >= '2020-01-01'  -- Data quality filter
    )

select *
from cleaned
