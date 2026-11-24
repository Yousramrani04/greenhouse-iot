-- Grain : un capteur.
select distinct sensor_id, greenhouse_id, metric, unit
from {{ ref('stg_measurements') }}
