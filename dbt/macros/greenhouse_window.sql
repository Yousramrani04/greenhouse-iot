{#- Agrège les minutes Silver par serre, mesure et fenêtre de N minutes.
    Moyenne pondérée par le nombre de mesures brutes de chaque minute.
    En incrémental : on ne recalcule que les fenêtres des `lookback_hours` dernières heures,
    ce qui absorbe les retardataires (30 min maximum) déjà corrigés en Silver par MERGE. -#}
{% macro greenhouse_window(minutes) %}
select
    greenhouse_id,
    metric,
    time_bucket(interval '{{ minutes }} minutes', minute_ts)  as bucket_start,
    sum(value * n_readings) / sum(n_readings)                 as avg_value,
    min(value)                                                as min_value,
    max(value)                                                as max_value,
    cast(count(*) as integer)                                 as n_minutes,
    cast(sum(n_readings) as bigint)                           as n_readings
from {{ ref('stg_measurements') }}
{% if is_incremental() %}
where time_bucket(interval '{{ minutes }} minutes', minute_ts)
      >= (select max(bucket_start) - interval '{{ var("lookback_hours") }} hours' from {{ this }})
{% endif %}
group by 1, 2, 3
{% endmacro %}
