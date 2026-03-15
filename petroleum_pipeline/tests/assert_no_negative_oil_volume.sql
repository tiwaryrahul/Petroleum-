-- Wells should never report negative oil volumes
select
    well_id,
    production_date,
    oil_volume_bbl
from {{ ref('silver_well_production') }}
where oil_volume_bbl < 0
