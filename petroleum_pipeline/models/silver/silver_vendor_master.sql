{{ config(materialized='view') }}

select
    vendor_id,
    initcap(trim(vendor_name)) as vendor_name,
    upper(trim(service_type)) as service_type,
    initcap(trim(location)) as location
from {{ ref('bronze_vendor_master') }}
where vendor_id is not null
