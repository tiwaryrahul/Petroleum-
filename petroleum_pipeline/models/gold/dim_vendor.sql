{{ config(materialized='table') }}

SELECT
    vendor_id,
    vendor_name,
    service_type,
    location
FROM {{ ref('bronze_vendor_master') }}
