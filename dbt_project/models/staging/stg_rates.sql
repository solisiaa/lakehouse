{{ config(materialized='view', schema='staging') }}

/* Staging курсов: только положительные курсы, верхние границы от выбросов. */
select
    cast(day as date) as day,
    cast(base as varchar) as base,
    cast(currency as varchar) as currency,
    cast(rate as double) as rate
from {{ source('silver', 'exchange_rates') }}
where day is not null
  and rate > 0
  and rate < 100000
