-- Outlet pressure should not exceed inlet pressure (negative pressure drop)
select
    pipeline_id,
    segment_id,
    reading_timestamp,
    pressure_drop_psi
from {{ ref('silver_pipeline_operations') }}
where pressure_drop_psi < 0
