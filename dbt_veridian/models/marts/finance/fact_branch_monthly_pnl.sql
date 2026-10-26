{{ config(materialized='table') }}

WITH branch_base AS (
    SELECT branch_key, branch_code, branch_name, region
    FROM {{ ref('dim_branch') }}
),

financials AS (
    SELECT
        upper(trim(branch_code)) AS branch_code,
        to_date(reporting_month, 'YYYY-MM-DD') AS reporting_month,
        cast(interest_income AS numeric(14,2)) AS interest_income,
        cast(operating_cost AS numeric(14,2)) AS operating_cost
    FROM {{ source('source', 'branch_financials') }}
)

SELECT
    b.branch_key,
    b.branch_code,
    coalesce(f.reporting_month, '2025-11-01'::date) AS reporting_month,
    f.interest_income,
    f.operating_cost,
    (f.interest_income - f.operating_cost) AS net_contribution,
    CASE 
        WHEN f.interest_income IS NULL OR f.operating_cost IS NULL THEN TRUE 
        ELSE FALSE 
    END AS financials_missing
FROM branch_base b
LEFT JOIN financials f ON b.branch_code = f.branch_code