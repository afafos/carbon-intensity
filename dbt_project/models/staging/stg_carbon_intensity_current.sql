{{
    config(
        materialized='view',
        tags=['staging', 'stg', 'current']
    )
}}

/*
    Модель: stg_carbon_intensity_current
    
    Описание: 
    Staging слой для актуальных версий данных о carbon intensity.
    Производит базовую очистку и нормализацию данных из raw слоя.
    
    Бизнес-логика:
    - Отбираются только актуальные версии (valid_to_dttm = 5999-01-01)
    - Добавляются вычисляемые поля (длительность периода, статус данных)
    - Форматируются индексы интенсивности
    
    Зависимости: raw.carbon_intensity
*/

WITH source_data AS (
    -- Выбираем только текущие версии данных (SCD Type 2)
    SELECT *
    FROM {{ source('raw', 'carbon_intensity') }}
    WHERE valid_to_dttm = '5999-01-01 00:00:00'
),

cleaned_data AS (
    SELECT
        -- Идентификаторы
        id,
        
        -- Временные метки периода данных
        data_from_dttm,
        data_to_dttm,
        
        -- Вычисляем длительность периода в минутах (обычно 30)
        EXTRACT(EPOCH FROM (data_to_dttm - data_from_dttm)) / 60 AS period_duration_minutes,
        
        -- Интенсивность CO2
        forecast_intensity,
        actual_intensity,
        
        -- Разница между прогнозом и фактом (если факт есть)
        -- Положительное значение = прогноз был выше факта
        CASE 
            WHEN actual_intensity IS NOT NULL 
            THEN forecast_intensity - actual_intensity
            ELSE NULL
        END AS forecast_error_gco2_kwh,
        
        -- Процент ошибки прогноза
        CASE 
            WHEN actual_intensity IS NOT NULL AND actual_intensity > 0
            THEN ROUND(
                ((forecast_intensity - actual_intensity)::NUMERIC / actual_intensity::NUMERIC) * 100, 
                2
            )
            ELSE NULL
        END AS forecast_error_percent,
        
        -- Индексы интенсивности (нормализованные)
        {% set intensity_mapping = {
            'very low': 1,
            'low': 2,
            'moderate': 3,
            'high': 4,
            'very high': 5
        } %}
        
        LOWER(TRIM(index_forecast)) AS index_forecast,
        CASE 
            {% for key, value in intensity_mapping.items() %}
            WHEN LOWER(TRIM(index_forecast)) = '{{ key }}' THEN {{ value }}
            {% endfor %}
            ELSE NULL
        END AS index_forecast_numeric,
        
        LOWER(TRIM(index_actual)) AS index_actual,
        CASE 
            {% for key, value in intensity_mapping.items() %}
            WHEN LOWER(TRIM(index_actual)) = '{{ key }}' THEN {{ value }}
            {% endfor %}
            ELSE NULL
        END AS index_actual_numeric,
        
        -- Статус данных
        CASE 
            WHEN actual_intensity IS NOT NULL THEN 'completed'
            WHEN data_from_dttm <= CURRENT_TIMESTAMP THEN 'in_progress'
            ELSE 'future'
        END AS data_status,
        
        -- Версионность
        valid_from_dttm,
        valid_to_dttm,
        
        -- Технические поля
        src_processed_dttm,
        processed_dttm,
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM source_data
)

SELECT * FROM cleaned_data

-- Фильтруем явно битые данные (если есть)
WHERE period_duration_minutes > 0
  AND period_duration_minutes <= 60  -- защита от аномальных периодов
  AND forecast_intensity >= 0  -- интенсивность не может быть отрицательной
  AND (actual_intensity IS NULL OR actual_intensity >= 0)
