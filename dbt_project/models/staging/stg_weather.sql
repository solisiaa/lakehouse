{{ config(materialized='view', schema='staging') }}

/* Staging погоды: лёгкая нормализация типов + фильтр мусора. */
select
    cast(day as date) as day,
    cast(city as varchar) as city,
    cast(avg_temp_c as double) as avg_temp_c,
    cast(max_temp_c as double) as max_temp_c,
    cast(min_temp_c as double) as min_temp_c,
    cast(avg_humidity as double) as avg_humidity,
    cast(n_obs as integer) as n_obs
from {{ source('silver', 'weather_daily') }}
where day is not null
  and city is not null
  and avg_temp_c between -60 and 60
