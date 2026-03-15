-- Water cut percentage should be between 0 and 100
select
    well_id,
    production_date,
    water_cut_pct
from {{ ref('silver_well_production') }}
where water_cut_pct < 0 or water_cut_pct > 100
