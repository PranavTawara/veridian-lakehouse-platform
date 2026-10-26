{{ config(materialized='table') }}

WITH silver_customers AS (
    SELECT * FROM {{ source('silver', 'customers') }}
)

SELECT
    md5(customer_id) AS customer_key,
    customer_id,
    customer_name,
    city,
    state,
    kyc_status,
    income_band,
    onboarding_date,
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM silver_customers
