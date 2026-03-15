{{ config(materialized='table') }}

SELECT
    subsidiary_id,
    subsidiary_name,
    functional_currency
FROM {{ ref('bronze_subsidiary_master') }}
