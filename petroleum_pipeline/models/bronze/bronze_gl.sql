{{ config(
    materialized='incremental',
    unique_key='journal_id'
) }}

SELECT *
FROM {{ source('bronze','general_ledger_transactions') }}

{% if is_incremental() %}
WHERE posting_date >
(
    SELECT MAX(posting_date)
    FROM {{ this }}
)
{% endif %}
