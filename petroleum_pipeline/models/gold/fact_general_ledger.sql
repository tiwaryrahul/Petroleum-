{{ config(materialized='table') }}

SELECT
    gl.journal_id,
    gl.line_id,
    gl.posting_date,
    gl.document_date,
    gl.subsidiary_id,
    gl.account_number,
    gl.customer_id,
    gl.vendor_id,
    gl.product_id,
    gl.quantity,
    COALESCE(gl.debit_amount, 0) AS debit_amount,
    COALESCE(gl.credit_amount, 0) AS credit_amount,
    COALESCE(gl.debit_amount, 0) - COALESCE(gl.credit_amount, 0) AS net_amount,
    gl.created_by,
    gl.approval_status,
    gl.error_flag,
    gl.error_type
FROM {{ ref('bronze_gl') }} gl
