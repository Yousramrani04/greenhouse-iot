{{ config(materialized='incremental',
          unique_key=['greenhouse_id', 'metric', 'bucket_start'],
          incremental_strategy='delete+insert') }}
-- Grain : une serre x une mesure x une fenêtre de 5 minutes (UTC).
{{ greenhouse_window(5) }}
