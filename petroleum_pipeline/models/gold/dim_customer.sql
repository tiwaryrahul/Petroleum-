{{ config(materialized='table') }}

SELECT
    customer_id,
    customer_name,
    credit_limit,
    industry,
    location
FROM {{ ref('bronze_customer_master') }}
