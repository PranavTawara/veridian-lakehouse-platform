{{ config(materialized='table') }}

SELECT
    md5(product_code) AS product_key,
    product_code,
    product_name,
    category,
    interest_rate
    , CURRENT_TIMESTAMP AS _loaded_at
FROM {{ source('silver', 'products') }}
