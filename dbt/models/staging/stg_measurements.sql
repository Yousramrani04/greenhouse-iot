-- Grain : un capteur sur une minute. Typage uniquement, aucune règle métier (déjà faites en Silver).
select
    sensor_id,
    greenhouse_id,
    metric,
    unit,
    cast(minute_ts as timestamp)         as minute_ts,        -- UTC, sans fuseau
    value,
    n_readings,
    cast(last_ingested_at as timestamp)  as last_ingested_at
from {{ source('silver', 'measurements_1min') }}
