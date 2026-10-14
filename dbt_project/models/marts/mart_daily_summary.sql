{{ config(materialized='table', schema='gold') }}

/* Демонстрационная витрина «день -> погода + доллар»: join фактов. */
with w as (
    select * from {{ ref('fact_weather') }}
),
r as (
    select * from {{ ref('fact_rates') }}
    where base = 'USD' and currency in ('EUR', 'RUB', 'CNY')
)
select
    w.day,
    w.city,
    w.avg_temp_c,
    w.avg_humidity,
    max(case when r.currency = 'EUR' then r.rate end) as usd_to_eur,
    max(case when r.currency = 'RUB' then r.rate end) as usd_to_rub,
    max(case when r.currency = 'CNY' then r.rate end) as usd_to_cny
from w
left join r on w.day = r.day
group by 1, 2, 3, 4
