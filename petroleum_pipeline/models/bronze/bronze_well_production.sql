{{ config(materialized='view') }}

select
    well_id,
    well_name,
    production_date,
    oil_volume_bbl,
    gas_volume_mcf,
    water_volume_bbl,
    wellhead_pressure_psi,
    wellhead_temperature_f,
    choke_size,
    status,
    _loaded_at
from {{ ref('seed_well_production') }}
