{{ config(materialized='view') }}

select
    gl.journal_id,
    gl.line_id,
    gl.posting_date,
    gl.document_date,
    gl.subsidiary_id,
    sub.subsidiary_name,
    sub.functional_currency,
    gl.account_number,
    coa.account_name,
    coa.account_type,
    coa.sub_type,
    coa.normal_balance,
    coa.financial_statement_category,
    coalesce(gl.debit_amount, 0) as debit_amount,
    coalesce(gl.credit_amount, 0) as credit_amount,
    coalesce(gl.debit_amount, 0) - coalesce(gl.credit_amount, 0) as net_amount,
    gl.customer_id,
    cust.customer_name,
    cust.industry as customer_industry,
    gl.vendor_id,
    vend.vendor_name,
    vend.service_type as vendor_service_type,
    gl.product_id,
    prod.product_name,
    prod.category as product_category,
    prod.standard_cost_per_barrel,
    gl.quantity,
    case
        when gl.quantity > 0 and prod.standard_cost_per_barrel is not null
        then gl.quantity * prod.standard_cost_per_barrel
        else null
    end as standard_cost_total,
    gl.created_by,
    upper(trim(gl.approval_status)) as approval_status,
    upper(trim(gl.error_flag)) as error_flag,
    gl.error_type,
    case
        when upper(trim(gl.error_flag)) = 'Y' then true
        else false
    end as is_error,
    case
        when upper(trim(gl.approval_status)) = 'APPROVED' then true
        else false
    end as is_approved
from {{ ref('bronze_gl') }} gl
left join {{ ref('bronze_subsidiary_master') }} sub
    on gl.subsidiary_id = sub.subsidiary_id
left join {{ ref('bronze_chart_of_accounts') }} coa
    on gl.account_number = coa.account_number
left join {{ ref('bronze_customer_master') }} cust
    on gl.customer_id = cust.customer_id
left join {{ ref('bronze_vendor_master') }} vend
    on gl.vendor_id = vend.vendor_id
left join {{ ref('bronze_product_master') }} prod
    on gl.product_id = prod.product_id
