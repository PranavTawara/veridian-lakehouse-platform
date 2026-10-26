-- Fails if any conformed silver transaction references a non-existent loan account
SELECT t.txn_id, t.account_id
FROM {{ source('silver', 'transactions') }} t
LEFT JOIN {{ source('silver', 'loans') }} l ON t.account_id = l.account_id
WHERE l.account_id IS NULL