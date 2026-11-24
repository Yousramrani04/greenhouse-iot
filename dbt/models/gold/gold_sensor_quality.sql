-- Grain : un capteur (sur toute la période disponible).
-- Fiabilité = disponibilité (minutes présentes / minutes attendues) pénalisée par la part de messages rejetés.
with avail as (
    select sensor_id,
           sum(present_minutes)  as present_minutes,
           sum(expected_minutes) as expected_minutes,
           sum(n_readings)       as valid_readings
    from {{ ref('gold_sensor_availability') }}
    group by 1
),
rej as (
    select sensor_id,
           count(*) filter (where reason = 'null')         as n_null,
           count(*) filter (where reason = 'sentinel')     as n_sentinel,
           count(*) filter (where reason = 'out_of_range') as n_out_of_range,
           count(*)                                        as n_rejected
    from {{ ref('stg_rejected') }}
    group by 1
),
base as (
    select
        s.sensor_id, s.greenhouse_id, s.metric,
        cast(coalesce(a.present_minutes, 0) as bigint)   as present_minutes,
        cast(coalesce(a.expected_minutes, 0) as bigint)  as expected_minutes,
        cast(coalesce(a.valid_readings, 0) as bigint)    as valid_readings,
        cast(coalesce(r.n_null, 0) as bigint)            as n_null,
        cast(coalesce(r.n_sentinel, 0) as bigint)        as n_sentinel,
        cast(coalesce(r.n_out_of_range, 0) as bigint)    as n_out_of_range,
        cast(coalesce(r.n_rejected, 0) as bigint)        as n_rejected
    from {{ ref('dim_sensor') }} s
    left join avail a on a.sensor_id = s.sensor_id
    left join rej r   on r.sensor_id = s.sensor_id
)
select
    *,
    round(100.0 * present_minutes / nullif(expected_minutes, 0), 2)         as availability_pct,
    round(100.0 * n_rejected / nullif(n_rejected + valid_readings, 0), 2)   as rejection_pct,
    round((100.0 * present_minutes / nullif(expected_minutes, 0))
          * (1 - n_rejected * 1.0 / nullif(n_rejected + valid_readings, 0)), 2) as reliability_pct
from base
