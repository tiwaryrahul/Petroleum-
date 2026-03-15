{{ config(materialized='table') }}

select
    production_date,
    count(distinct well_id) as active_wells,
    sum(oil_volume_bbl) as total_oil_bbl,
    sum(gas_volume_mcf) as total_gas_mcf,
    sum(water_volume_bbl) as total_water_bbl,
    sum(total_liquid_bbl) as total_liquid_bbl,
    avg(water_cut_pct) as avg_water_cut_pct,
    avg(wellhead_pressure_psi) as avg_wellhead_pressure_psi
from {{ ref('silver_well_production') }}
group by production_date
