{{ config(materialized='table') }}

SELECT
    product_id,
    product_name,
    category,
    standard_cost_per_barrel
FROM {{ ref('bronze_product_master') }}
