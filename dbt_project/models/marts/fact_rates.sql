{{ config(materialized='incremental', schema='gold', unique_key='day_base_currency') }}

/* Витрина курсов: incremental по day (перезаливка последних 7 дней). */
select
    day,
    base,
    currency,
    rate,
    concat(cast(day as varchar), '_', base, '_', currency) as day_base_currency,
    current_timestamp as loaded_at
from {{ ref('stg_rates') }}
{% if is_incremental() %}
where day >= date_add('day', -7, current_date)
{% endif %}
