-- Fails if branch BR-047 is omitted or not marked as missing
SELECT branch_code, financials_missing
FROM {{ ref('fact_branch_monthly_pnl') }}
WHERE branch_code = 'BR-047'
  AND (financials_missing IS NOT TRUE OR net_contribution IS NOT NULL)