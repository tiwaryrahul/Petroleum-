{{ config(materialized='view') }}

select
    pipeline_id,
    pipeline_name,
    segment_id,
    reading_timestamp,
    inlet_pressure_psi,
    outlet_pressure_psi,
    flow_rate_bpd,
    temperature_f,
    status,
    _loaded_at
from {{ ref('seed_pipeline_operations') }}
