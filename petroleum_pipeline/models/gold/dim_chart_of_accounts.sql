{{ config(materialized='table') }}

SELECT
    account_number,
    account_name,
    account_type,
    sub_type,
    normal_balance,
    financial_statement_category
FROM {{ ref('bronze_chart_of_accounts') }}
