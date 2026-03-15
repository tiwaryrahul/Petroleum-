SELECT journal_entry_id

FROM {{ ref('gl_transactions_enriched') }}

GROUP BY journal_entry_id

HAVING SUM(debit_amount) != SUM(credit_amount)
