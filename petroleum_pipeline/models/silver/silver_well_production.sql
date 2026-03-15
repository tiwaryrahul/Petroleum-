{{ config(materialized='view') }}

select
    well_id,
    well_name,
    production_date,
    oil_volume_bbl,
    gas_volume_mcf,
    water_volume_bbl,
    oil_volume_bbl + water_volume_bbl as total_liquid_bbl,
    case
        when (oil_volume_bbl + water_volume_bbl) > 0
        then water_volume_bbl / (oil_volume_bbl + water_volume_bbl) * 100
        else 0
    end as water_cut_pct,
    wellhead_pressure_psi,
    wellhead_temperature_f,
    choke_size,
    status
from {{ ref('bronze_well_production') }}
where status is not null
