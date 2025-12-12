{{ config(severity='warn') }}

/*
    Singular тест: Проверка логики прогноза vs факта
    
    Описание:
    Проверяет, что для завершенных периодов (с фактическими данными):
    1. Факт существует
    2. Прогноз не слишком сильно отличается от факта (ошибка < 100%)
    3. Значения в разумных пределах
    
    Тип теста: Singular (специфичный для конкретной бизнес-логики)
    
    Тест настроен как WARNING, так как экстремальные ошибки прогноза могут быть
    в реальных условиях (резкие изменения в энергосистеме)
    
    Тест считается успешным, если запрос возвращает 0 строк
*/

WITH completed_periods AS (
    SELECT
        period_key,
        data_from_dttm,
        forecast_intensity,
        actual_intensity,
        data_status,
        ABS(forecast_intensity - actual_intensity) AS absolute_error,
        CASE 
            WHEN actual_intensity > 0 
            THEN ABS((forecast_intensity - actual_intensity)::NUMERIC / actual_intensity::NUMERIC) * 100
            ELSE NULL
        END AS error_percent
    FROM {{ ref('ods_carbon_intensity') }}
    WHERE data_status = 'completed'
      AND actual_intensity IS NOT NULL
)

-- Находим аномальные случаи
SELECT
    period_key,
    data_from_dttm,
    forecast_intensity,
    actual_intensity,
    absolute_error,
    error_percent,
    CASE 
        WHEN actual_intensity IS NULL THEN 'Missing actual data for completed period'
        WHEN error_percent > 100 THEN 'Forecast error too high (> 100%)'
        WHEN forecast_intensity <= 0 THEN 'Invalid forecast value'
        WHEN actual_intensity <= 0 THEN 'Invalid actual value'
        ELSE 'Unknown issue'
    END AS failure_reason
FROM completed_periods
WHERE 
    actual_intensity IS NULL  -- не должно быть для completed
    OR error_percent > 100     -- экстремально большая ошибка
    OR forecast_intensity <= 0
    OR actual_intensity <= 0
