{{ config(materialized='table') }}

select
    pipeline_id,
    pipeline_name,
    count(distinct segment_id) as total_segments,
    avg(pressure_drop_psi) as avg_pressure_drop_psi,
    max(pressure_drop_psi) as max_pressure_drop_psi,
    avg(flow_rate_bpd) as avg_flow_rate_bpd,
    sum(case when pressure_drop_severity = 'HIGH' then 1 else 0 end) as high_severity_count,
    sum(case when pressure_drop_severity = 'MEDIUM' then 1 else 0 end) as medium_severity_count
from {{ ref('silver_pipeline_operations') }}
group by pipeline_id, pipeline_name
