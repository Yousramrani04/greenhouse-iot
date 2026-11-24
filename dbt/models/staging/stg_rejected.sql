-- Un message rejeté en Silver (raison : null | sentinel | out_of_range). Typage uniquement.
select
    sensor_id,
    greenhouse_id,
    metric,
    cast(event_time as timestamp)   as event_time,     -- UTC, sans fuseau
    raw_value,
    raw_unit,
    reason,
    cast(ingested_at as timestamp)  as ingested_at
from {{ source('silver', 'measurements_rejected') }}
