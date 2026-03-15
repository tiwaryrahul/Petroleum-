{{ config(materialized='view') }}

select
    subsidiary_id,
    initcap(trim(subsidiary_name)) as subsidiary_name,
    upper(trim(functional_currency)) as functional_currency
from {{ ref('bronze_subsidiary_master') }}
where subsidiary_id is not null
