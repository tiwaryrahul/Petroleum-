{{ config(
    materialized='incremental',
    unique_key='subsidiary_id'
) }}

SELECT *
FROM {{ source('bronze', 'subsidiary_master') }}

{% if is_incremental() %}
WHERE subsidiary_id NOT IN (
    SELECT subsidiary_id FROM {{ this }}
)
{% endif %}
