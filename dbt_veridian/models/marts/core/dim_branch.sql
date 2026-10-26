{{ config(materialized='table') }}

SELECT
    md5(branch_code) AS branch_key,
    branch_code,
    branch_name,
    region,
    state,
    city,
    branch_type,
    open_date
    , CURRENT_TIMESTAMP AS _loaded_at
FROM {{ source('silver', 'branches') }}
