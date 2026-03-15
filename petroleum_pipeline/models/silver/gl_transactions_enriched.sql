{{ config(materialized='table') }}

SELECT

    gl.journal_id AS journal_entry_id,
    gl.line_id,
    gl.posting_date,

    gl.account_number,
    coa.account_name,
    coa.account_type,

    gl.debit_amount,
    gl.credit_amount,

    gl.customer_id,
    cm.customer_name,

    gl.vendor_id,
    vm.vendor_name,

    gl.product_id,
    pm.product_name,

    pm.standard_cost_per_barrel,

    gl.quantity,
    gl.created_by

FROM {{ source('bronze','general_ledger_transactions') }} gl

LEFT JOIN {{ source('bronze','chart_of_accounts') }} coa
    ON gl.account_number = coa.account_number

LEFT JOIN {{ source('bronze','customer_master') }} cm
    ON gl.customer_id = cm.customer_id

LEFT JOIN {{ source('bronze','vendor_master') }} vm
    ON gl.vendor_id = vm.vendor_id

LEFT JOIN {{ source('bronze','product_master') }} pm
    ON gl.product_id = pm.product_id
