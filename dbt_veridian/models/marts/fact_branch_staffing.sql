{{ config(materialized='table') }}

WITH active_staff AS (
    SELECT
        branch_code,
        count(employee_id) AS total_active_staff,
        count(CASE WHEN role ILIKE '%LOAN%' OR role ILIKE '%CREDIT%' OR role ILIKE '%RELATIONSHIP%' THEN 1 END) AS loan_officers,
        count(CASE WHEN role ILIKE '%MANAGER%' OR role ILIKE '%HEAD%' THEN 1 END) AS branch_managers,
        round(coalesce(sum(annual_ctc), 0)::numeric, 2) AS total_annual_payroll,
        round(coalesce(avg(annual_ctc), 0)::numeric, 2) AS avg_employee_ctc
    FROM {{ ref('dim_employee') }}
    WHERE employment_status = 'ACTIVE'
    GROUP BY branch_code
),

branch_loans AS (
    SELECT
        branch_code,
        count(account_id) AS total_loans_serviced,
        coalesce(sum(outstanding_amount), 0) AS total_outstanding_book,
        count(CASE WHEN is_regulatory_npa = TRUE THEN 1 END) AS total_npa_accounts,
        coalesce(sum(CASE WHEN is_regulatory_npa = TRUE THEN outstanding_amount ELSE 0 END), 0) AS total_npa_balance
    FROM {{ source('silver', 'loans') }}
    GROUP BY branch_code
)

SELECT
    b.branch_key,
    b.branch_code,
    b.branch_name,
    b.region,
    b.branch_type,
    coalesce(s.total_active_staff, 0) AS total_active_staff,
    coalesce(s.loan_officers, 0) AS loan_officers,
    coalesce(s.branch_managers, 0) AS branch_managers,
    coalesce(s.total_annual_payroll, 0) AS total_annual_payroll,
    coalesce(s.avg_employee_ctc, 0) AS avg_employee_ctc,
    coalesce(l.total_loans_serviced, 0) AS total_loans_serviced,
    round(coalesce(l.total_outstanding_book, 0)::numeric, 2) AS total_outstanding_book,
    CASE 
        WHEN coalesce(s.loan_officers, 0) > 0 
        THEN round((coalesce(l.total_loans_serviced, 0)::numeric / s.loan_officers), 2)
        ELSE 0 
    END AS loans_per_officer,
    CASE 
        WHEN coalesce(s.total_active_staff, 0) > 0 
        THEN round((coalesce(l.total_outstanding_book, 0)::numeric / s.total_active_staff), 2)
        ELSE 0 
    END AS book_size_per_employee,
    CURRENT_TIMESTAMP AS dbt_loaded_at
FROM {{ ref('dim_branch') }} b
LEFT JOIN active_staff s ON b.branch_code = s.branch_code
LEFT JOIN branch_loans l ON b.branch_code = l.branch_code
