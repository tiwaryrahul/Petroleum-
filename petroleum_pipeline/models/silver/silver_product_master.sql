{{ config(materialized='view') }}

select
    product_id,
    initcap(trim(product_name)) as product_name,
    upper(trim(category)) as category,
    coalesce(standard_cost_per_barrel, 0) as standard_cost_per_barrel
from {{ ref('bronze_product_master') }}
where product_id is not null
