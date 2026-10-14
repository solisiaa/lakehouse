{{ config(materialized='table', schema='gold') }}

/* Витрина погоды: факты по дням + флаги аномалий. */
with base as (
    select * from {{ ref('stg_weather') }}
)
select
    day,
    city,
    avg_temp_c,
    max_temp_c,
    min_temp_c,
    max_temp_c - min_temp_c as diurnal_range_c,
    avg_humidity,
    n_obs,
    case when avg_temp_c < -10 or avg_temp_c > 30 then true else false end as is_extreme,
    current_timestamp as loaded_at
from base
