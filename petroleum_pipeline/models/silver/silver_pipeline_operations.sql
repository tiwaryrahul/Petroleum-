{{ config(materialized='view') }}

select
    pipeline_id,
    pipeline_name,
    segment_id,
    reading_timestamp,
    inlet_pressure_psi,
    outlet_pressure_psi,
    inlet_pressure_psi - outlet_pressure_psi as pressure_drop_psi,
    flow_rate_bpd,
    temperature_f,
    status,
    case
        when inlet_pressure_psi - outlet_pressure_psi > 50 then 'HIGH'
        when inlet_pressure_psi - outlet_pressure_psi > 20 then 'MEDIUM'
        else 'NORMAL'
    end as pressure_drop_severity
from {{ ref('bronze_pipeline_operations') }}
where status is not null
