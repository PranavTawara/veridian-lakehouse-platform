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
FROM {{ source('silver', 'branches') }}