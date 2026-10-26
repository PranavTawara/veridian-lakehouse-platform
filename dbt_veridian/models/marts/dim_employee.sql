{{ config(materialized='table') }}

WITH silver_employees AS (
    SELECT * FROM {{ source('silver', 'employees') }}
)

SELECT
    md5(employee_id) AS employee_key,
    employee_id,
    employee_name,
    branch_code,
    branch_name,
    role,
    join_date,
    exit_date,
    status AS employment_status,
    annual_ctc,
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM silver_employees
