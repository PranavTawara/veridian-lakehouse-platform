-- Reconciles disbursement transactions against the principal of disbursed loans
WITH totals AS (
    SELECT
        (
            SELECT coalesce(sum(clean_amount), 0) 
            FROM {{ source('silver', 'transactions') }} 
            WHERE txn_type = 'DISBURSEMENT'
        ) AS total_disbursed,
        (
            SELECT coalesce(sum(principal_amount), 0) 
            FROM {{ source('silver', 'loans') }} 
            WHERE account_id IN (
                SELECT DISTINCT account_id 
                FROM {{ source('silver', 'transactions') }} 
                WHERE txn_type = 'DISBURSEMENT'
            )
        ) AS total_principal
)
SELECT
    total_disbursed,
    total_principal,
    abs(total_disbursed - total_principal) AS variance,
    round((abs(total_disbursed - total_principal) / nullif(total_principal, 0) * 100)::numeric, 4) AS variance_pct
FROM totals
WHERE round((abs(total_disbursed - total_principal) / nullif(total_principal, 0) * 100)::numeric, 4) > 0.01
