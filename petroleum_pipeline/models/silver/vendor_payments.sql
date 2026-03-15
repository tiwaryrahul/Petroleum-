{{ config(materialized='table') }}

SELECT

    journal_entry_id,
    vendor_id,
    vendor_name,

    SUM(debit_amount) AS payment_amount,

    COUNT(*) AS transactions

FROM {{ ref('gl_transactions_enriched') }}

WHERE vendor_id IS NOT NULL

GROUP BY
    journal_entry_id,
    vendor_id,
    vendor_name
