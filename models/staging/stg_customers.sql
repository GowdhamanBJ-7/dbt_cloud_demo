{{ config(materialized='view') }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_ecommerce', 'customers') }}
),

cleaned AS (
    SELECT
        customer_id,
        TRIM(LOWER(email)) AS email,
        TRIM(first_name) AS first_name,
        TRIM(last_name) AS last_name,
        CASE 
            WHEN phone RLIKE '^[0-9+\-\s\(\)]+$' THEN phone 
            ELSE NULL 
        END AS phone,
        registration_date,
        CASE 
            WHEN country_code IN ('US', 'CA', 'UK', 'DE', 'FR', 'AU') 
            THEN country_code 
            ELSE 'OTHER' 
        END AS country_code,
        date_of_birth,
        CASE 
            WHEN gender IN ('M', 'F', 'Male', 'Female') 
            THEN UPPER(LEFT(gender, 1)) 
            ELSE 'U' 
        END AS gender,
        is_active,
        created_at,
        updated_at
    FROM source_data
    WHERE email IS NOT NULL
      AND registration_date >= '2020-01-01'  -- Data quality filter
)

SELECT * FROM cleaned
