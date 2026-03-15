{{ config(materialized='view') }}

select
    journal_id,
    line_id,
    posting_date,
    document_date,
    subsidiary_id,
    account_number,
    account_name,
    debit_amount,
    credit_amount,
    customer_id,
    vendor_id,
    product_id,
    quantity,
    created_by,
    approval_status,
    error_flag,
    error_type
from {{ source('bronze', 'general_ledger_transactions') }}
