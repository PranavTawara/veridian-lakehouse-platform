{{ config(materialized='table') }}

WITH loan_summary AS (
    SELECT
        b.region,
        count(l.account_id) AS total_accounts,
        sum(l.outstanding_amount) AS total_book_balance,
        sum(CASE WHEN l.is_regulatory_npa THEN l.outstanding_amount ELSE 0 END) AS npa_balance,
        sum(CASE WHEN l.is_regulatory_npa THEN 1 ELSE 0 END) AS npa_accounts
    FROM {{ source('silver', 'loans') }} l
    INNER JOIN {{ ref('dim_branch') }} b ON l.branch_code = b.branch_code
    GROUP BY b.region
)

SELECT
    region,
    total_accounts,
    total_book_balance,
    npa_balance,
    npa_accounts,
    round((npa_balance / nullif(total_book_balance, 0) * 100)::numeric, 2) AS npa_rate_pct,
    CASE
        WHEN (npa_balance / nullif(total_book_balance, 0) * 100) < 3.0 THEN 'GREEN'
        WHEN (npa_balance / nullif(total_book_balance, 0) * 100) <= 5.0 THEN 'AMBER'
        ELSE 'RED'
    END AS rag_status
    , CURRENT_TIMESTAMP AS _loaded_at
FROM loan_summary
