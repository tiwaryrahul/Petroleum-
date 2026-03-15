{{ config(materialized='table') }}

SELECT
    gl.journal_id,
    gl.posting_date,
    gl.vendor_id,
    gl.subsidiary_id,
    gl.account_number,
    COALESCE(gl.debit_amount, 0) AS payment_amount,
    gl.quantity,
    gl.created_by,
    gl.approval_status
FROM {{ ref('bronze_gl') }} gl
WHERE gl.vendor_id IS NOT NULL
    AND COALESCE(gl.debit_amount, 0) > 0
