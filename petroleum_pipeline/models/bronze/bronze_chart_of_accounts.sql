{{ config(
    materialized='incremental',
    unique_key='account_number'
) }}

SELECT *
FROM {{ source('bronze', 'chart_of_accounts') }}

{% if is_incremental() %}
WHERE account_number NOT IN (
    SELECT account_number FROM {{ this }}
)
{% endif %}
