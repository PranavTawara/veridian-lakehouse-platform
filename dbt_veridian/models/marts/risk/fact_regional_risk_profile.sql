{{ config(materialized='table') }}

WITH loan_summary AS (
    SELECT
        b.region,
        count(l.account_id) AS total_accounts,
        sum(l.outstanding_amount) AS total_book_balance,
        -- Regulatory NPA (DPD >= 90 / CBS Default)
        sum(CASE WHEN l.is_regulatory_npa THEN l.outstanding_amount ELSE 0 END) AS npa_balance,
        sum(CASE WHEN l.is_regulatory_npa THEN 1 ELSE 0 END) AS npa_accounts,
        -- Early Delinquency Watchlist (SMA-0: 1-30 DPD)
        sum(CASE WHEN l.dpd BETWEEN 1 AND 30 THEN l.outstanding_amount ELSE 0 END) AS sma0_balance,
        sum(CASE WHEN l.dpd BETWEEN 1 AND 30 THEN 1 ELSE 0 END) AS sma0_accounts
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
    sma0_balance,
    sma0_accounts,
    round((sma0_balance / nullif(total_book_balance, 0) * 100)::numeric, 2) AS sma0_rate_pct,
    -- RAG Status: Dynamically calibrated against early delinquency concentration
    CASE
        WHEN (npa_balance / nullif(total_book_balance, 0) * 100) > 5.0 OR (sma0_balance / nullif(total_book_balance, 0) * 100) >= 15.5 THEN 'RED'
        WHEN (npa_balance / nullif(total_book_balance, 0) * 100) > 2.0 OR (sma0_balance / nullif(total_book_balance, 0) * 100) >= 14.6 THEN 'AMBER'
        ELSE 'GREEN'
    END AS rag_status,
    CURRENT_TIMESTAMP AS _loaded_at
FROM loan_summary
