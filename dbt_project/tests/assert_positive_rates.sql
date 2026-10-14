-- Кастомный тест: нет отрицательных курсов
select *
from {{ ref('fact_rates') }}
where rate <= 0
