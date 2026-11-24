-- Grain : une période continue de dépassement de température dans une serre.
-- Une période est coupée par une minute sous le seuil OU par un trou de plus de
-- `alert_gap_minutes` minutes. `is_alert` = durée >= `alert_min_minutes` (règle IOT-07).
with minutes as (
    select greenhouse_id, minute_ts, value,
           value > {{ var('temp_alert_c') }} as hot
    from {{ ref('stg_measurements') }}
    where metric = 'temperature'
),
marked as (
    select *,
           case when hot is distinct from lag(hot) over w
                  or minute_ts - lag(minute_ts) over w > interval '{{ var("alert_gap_minutes") }} minutes'
                then 1 else 0 end as new_period
    from minutes
    window w as (partition by greenhouse_id order by minute_ts)
),
numbered as (
    select *, sum(new_period) over (partition by greenhouse_id order by minute_ts) as period_id
    from marked
)
select
    greenhouse_id,
    min(minute_ts)                                              as start_ts,
    max(minute_ts)                                              as end_ts,
    date_diff('minute', min(minute_ts), max(minute_ts)) + 1     as duration_minutes,
    max(value)                                                  as peak_value,
    avg(value)                                                  as avg_value,
    cast(count(*) as integer)                                   as n_minutes,
    date_diff('minute', min(minute_ts), max(minute_ts)) + 1 >= {{ var('alert_min_minutes') }} as is_alert
from numbered
where hot
group by greenhouse_id, period_id
