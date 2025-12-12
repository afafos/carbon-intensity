/*
    Singular тест: Проверка свежести данных
    
    Описание:
    Проверяет, что данные обновляются регулярно и нет больших разрывов во времени.
    Для carbon intensity ожидаем данные каждые 30 минут.
    
    Тип теста: Singular (специфичная проверка для временных рядов)
    
    Тест считается успешным, если запрос возвращает 0 строк
*/

WITH time_gaps AS (
    SELECT
        data_from_dttm,
        LAG(data_from_dttm) OVER (ORDER BY data_from_dttm) AS prev_period,
        EXTRACT(EPOCH FROM (
            data_from_dttm - LAG(data_from_dttm) OVER (ORDER BY data_from_dttm)
        )) / 60 AS gap_minutes
    FROM {{ ref('ods_carbon_intensity') }}
    WHERE data_date >= CURRENT_DATE - INTERVAL '7 days'  -- проверяем последние 7 дней
)

-- Находим большие разрывы (больше 2 часов)
SELECT
    prev_period,
    data_from_dttm,
    gap_minutes,
    'Gap in data > 2 hours' AS failure_reason
FROM time_gaps
WHERE gap_minutes > 120  -- больше 2 часов между периодами
  AND prev_period IS NOT NULL
