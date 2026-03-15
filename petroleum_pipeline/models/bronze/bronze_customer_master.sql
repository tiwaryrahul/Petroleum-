{{ config(
    materialized='incremental',
    unique_key='customer_id'
) }}

SELECT *
FROM {{ source('bronze', 'customer_master') }}

{% if is_incremental() %}
WHERE customer_id NOT IN (
    SELECT customer_id FROM {{ this }}
)
{% endif %}
