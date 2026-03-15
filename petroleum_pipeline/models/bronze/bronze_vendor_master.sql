{{ config(
    materialized='incremental',
    unique_key='vendor_id'
) }}

SELECT *
FROM {{ source('bronze', 'vendor_master') }}

{% if is_incremental() %}
WHERE vendor_id NOT IN (
    SELECT vendor_id FROM {{ this }}
)
{% endif %}
