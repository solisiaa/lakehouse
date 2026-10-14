-- Кастомный тест: температура в физически разумных пределах
select *
from {{ ref('fact_weather') }}
where avg_temp_c < -60 or avg_temp_c > 60
