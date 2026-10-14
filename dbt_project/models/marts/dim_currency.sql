{{ config(materialized='table', schema='gold') }}

/* Справочник валют. */
with currencies as (
    select distinct currency from {{ ref('stg_rates') }}
)
select
    currency as currency_code,
    case currency
        when 'EUR' then 'Euro'
        when 'RUB' then 'Russian Ruble'
        when 'CNY' then 'Chinese Yuan'
        when 'GBP' then 'British Pound'
        when 'JPY' then 'Japanese Yen'
        when 'KZT' then 'Kazakhstani Tenge'
        when 'BYN' then 'Belarusian Ruble'
        else currency
    end as currency_name,
    current_timestamp as loaded_at
from currencies
