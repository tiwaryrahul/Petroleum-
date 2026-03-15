{{ config(
    materialized='incremental',
    unique_key='product_id'
) }}

SELECT *
FROM {{ source('bronze', 'product_master') }}

{% if is_incremental() %}
WHERE product_id NOT IN (
    SELECT product_id FROM {{ this }}
)
{% endif %}
