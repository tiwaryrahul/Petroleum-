{{ config(materialized='view') }}

select
    customer_id,
    initcap(trim(customer_name)) as customer_name,
    coalesce(credit_limit, 0) as credit_limit,
    case
        when credit_limit >= 1000000 then 'TIER_1'
        when credit_limit >= 500000 then 'TIER_2'
        when credit_limit >= 100000 then 'TIER_3'
        else 'TIER_4'
    end as credit_tier,
    upper(trim(industry)) as industry,
    initcap(trim(location)) as location
from {{ ref('bronze_customer_master') }}
where customer_id is not null
