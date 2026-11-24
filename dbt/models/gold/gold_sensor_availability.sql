-- Grain : un capteur x un jour (UTC).
-- Minutes attendues = étendue réellement observée ce jour-là sur TOUS les capteurs
-- (le jour en cours est partiel ; un jour sans aucune donnée n'apparaît pas).
with days as (
    select
        cast(minute_ts as date)                                   as day,
        date_diff('minute', min(minute_ts), max(minute_ts)) + 1   as expected_minutes
    from {{ ref('stg_measurements') }}
    group by 1
),
present as (
    select sensor_id, cast(minute_ts as date) as day,
           count(*) as present_minutes, sum(n_readings) as n_readings
    from {{ ref('stg_measurements') }}
    group by 1, 2
)
select
    s.sensor_id,
    s.greenhouse_id,
    s.metric,
    d.day,
    d.expected_minutes,
    cast(coalesce(p.present_minutes, 0) as integer)               as present_minutes,
    cast(coalesce(p.n_readings, 0) as bigint)                     as n_readings,
    round(100.0 * coalesce(p.present_minutes, 0) / d.expected_minutes, 2) as availability_pct
from {{ ref('dim_sensor') }} s
cross join days d
left join present p on p.sensor_id = s.sensor_id and p.day = d.day
