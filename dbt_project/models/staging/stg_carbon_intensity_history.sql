{{
    config(
        materialized='view',
        tags=['staging', 'stg', 'history']
    )
}}

/*
    Модель: stg_carbon_intensity_history
    
    Описание:
    Staging слой для ИСТОРИЧЕСКИХ версий данных о carbon intensity.
    Показывает, как менялись прогнозы для одних и тех же периодов времени.
    
    Бизнес-логика:
    - Все версии данных (включая устаревшие)
    - Вычисление горизонта прогнозирования
    - Анализ изменений прогнозов между версиями
    
    Зависимости: raw.carbon_intensity
*/

WITH source_data AS (
    -- Берем ВСЕ версии данных для анализа изменений
    SELECT *
    FROM {{ source('raw', 'carbon_intensity') }}
),

{% set version_types = {
    'current': 'is_current_version',
    'historical': 'is_historical_version'
} %}

enriched_data AS (
    SELECT
        -- Идентификаторы
        id,
        
        -- Временные метки
        data_from_dttm,
        data_to_dttm,
        
        -- Интенсивность
        forecast_intensity,
        actual_intensity,
        
        -- Индексы
        LOWER(TRIM(index_forecast)) AS index_forecast,
        LOWER(TRIM(index_actual)) AS index_actual,
        
        -- Версионность
        valid_from_dttm AS forecast_made_at,
        valid_to_dttm AS forecast_valid_until,
        
        -- Горизонт прогнозирования (сколько часов вперед был сделан прогноз)
        EXTRACT(EPOCH FROM (data_from_dttm - valid_from_dttm)) / 3600 AS forecast_horizon_hours,
        
        -- Флаги типа версии
        {% for key, field in version_types.items() %}
        CASE 
            WHEN valid_to_dttm = '5999-01-01 00:00:00' THEN {{ 'TRUE' if key == 'current' else 'FALSE' }}
            ELSE {{ 'FALSE' if key == 'current' else 'TRUE' }}
        END AS {{ field }},
        {% endfor %}
        
        -- Технические поля
        src_processed_dttm,
        processed_dttm,
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM source_data
),

-- Добавляем оконные функции для анализа изменений прогнозов
version_changes AS (
    SELECT
        *,
        
        -- Номер версии для каждого периода данных (1 = самый ранний прогноз)
        ROW_NUMBER() OVER (
            PARTITION BY data_from_dttm, data_to_dttm 
            ORDER BY forecast_made_at
        ) AS version_number,
        
        -- Всего версий для этого периода
        COUNT(*) OVER (
            PARTITION BY data_from_dttm, data_to_dttm
        ) AS total_versions,
        
        -- Предыдущее значение прогноза (для анализа изменений)
        LAG(forecast_intensity) OVER (
            PARTITION BY data_from_dttm, data_to_dttm 
            ORDER BY forecast_made_at
        ) AS prev_forecast_intensity,
        
        -- Изменение прогноза по сравнению с предыдущей версией
        forecast_intensity - LAG(forecast_intensity) OVER (
            PARTITION BY data_from_dttm, data_to_dttm 
            ORDER BY forecast_made_at
        ) AS forecast_change_from_prev
        
    FROM enriched_data
)

SELECT * FROM version_changes
