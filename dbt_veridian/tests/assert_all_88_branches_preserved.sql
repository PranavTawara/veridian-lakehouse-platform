-- Fails if total distinct conformed branches is not exactly 85
WITH branch_counts AS (
    SELECT count(*) AS total_branches FROM {{ ref('dim_branch') }}
)
SELECT * FROM branch_counts WHERE total_branches != 85
