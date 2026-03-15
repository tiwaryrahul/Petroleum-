{{ config(materialized='view') }}

select
    account_number,
    initcap(trim(account_name)) as account_name,
    upper(trim(account_type)) as account_type,
    upper(trim(sub_type)) as sub_type,
    upper(trim(normal_balance)) as normal_balance,
    upper(trim(financial_statement_category)) as financial_statement_category,
    case
        when upper(trim(financial_statement_category)) = 'BALANCE SHEET' then 'BS'
        when upper(trim(financial_statement_category)) = 'INCOME STATEMENT' then 'IS'
        else 'OTHER'
    end as statement_code
from {{ ref('bronze_chart_of_accounts') }}
where account_number is not null
