-- Grain : un capteur x un jour (UTC).
-- Minutes attendues = minutes où la plateforme était ACTIVE, c'est-à-dire où au moins la moitié des capteurs
-- ont émis (variable active_sensors_share). Une minute où presque aucun capteur n'émet correspond à un arrêt
-- de la source (producteur arrêté), pas à la panne d'un capteur : elle est détectée par la fraîcheur Soda live.
with per_minute as (
    select minute_ts, count(distinct sensor_id) as n_sensors
    from {{ ref('stg_measurements') }}
    group by 1
),
active_minutes as (
    select minute_ts
    from per_minute
    where n_sensors >= {{ var('active_sensors_share', 0.5) }} * (select count(*) from {{ ref('dim_sensor') }})
),
days as (
    select cast(minute_ts as date) as day, count(*) as expected_minutes
    from active_minutes
    group by 1
),
present as (
    select m.sensor_id, cast(m.minute_ts as date) as day,
           count(*) as present_minutes, sum(m.n_readings) as n_readings
    from {{ ref('stg_measurements') }} m
    join active_minutes a using (minute_ts)
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
