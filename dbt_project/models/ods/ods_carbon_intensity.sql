{{
    config(
        materialized='incremental',
        unique_key='period_key',
        incremental_strategy='delete+insert',
        on_schema_change='fail',
        tags=['ods', 'incremental']
    )
}}

/*
    Модель: ods_carbon_intensity
    
    Описание:
    ODS (Operational Data Store) слой для оперативного хранилища данных carbon intensity.
    Использует инкрементальную загрузку по стратегии DELETE+INSERT.
    
    Инкрементальная стратегия: DELETE+INSERT
    - Удаляет существующие записи с теми же ключами
    - Вставляет новые/обновленные записи
    - Подходит для небольших объемов данных с частыми обновлениями
    
    Бизнес-логика:
    - Хранит только актуальные версии данных
    - Добавляет бизнес-атрибуты и категоризацию
    - Обогащает данные дополнительными метриками
    
    Зависимости: stg_carbon_intensity_current
*/

WITH staging_data AS (
    SELECT *
    FROM {{ ref('stg_carbon_intensity_current') }}
    
    {% if is_incremental() %}
    -- Инкрементальная загрузка: только новые или обновленные данные
    -- Загружаем данные, которые были обработаны после последней загрузки
    WHERE processed_dttm > (SELECT MAX(dbt_updated_at) FROM {{ this }})
    {% endif %}
),

{% set intensity_categories = [
    ('very low', 0, 100),
    ('low', 100, 150),
    ('moderate', 150, 250),
    ('high', 250, 350),
    ('very high', 350, 9999)
] %}

enriched_data AS (
    SELECT
        -- Технические ключи
        id AS source_id,
        {{ dbt_utils.generate_surrogate_key(['data_from_dttm', 'data_to_dttm']) }} AS period_key,
        
        -- Временные поля
        data_from_dttm,
        data_to_dttm,
        DATE(data_from_dttm) AS data_date,
        EXTRACT(YEAR FROM data_from_dttm) AS data_year,
        EXTRACT(MONTH FROM data_from_dttm) AS data_month,
        EXTRACT(DAY FROM data_from_dttm) AS data_day,
        EXTRACT(HOUR FROM data_from_dttm) AS data_hour,
        TO_CHAR(data_from_dttm, 'Day') AS data_day_of_week,
        EXTRACT(ISODOW FROM data_from_dttm) AS data_day_of_week_num,
        
        -- Категории времени суток
        CASE 
            WHEN EXTRACT(HOUR FROM data_from_dttm) BETWEEN 6 AND 11 THEN 'morning'
            WHEN EXTRACT(HOUR FROM data_from_dttm) BETWEEN 12 AND 17 THEN 'afternoon'
            WHEN EXTRACT(HOUR FROM data_from_dttm) BETWEEN 18 AND 22 THEN 'evening'
            ELSE 'night'
        END AS time_of_day,
        
        -- Тип дня
        CASE 
            WHEN EXTRACT(ISODOW FROM data_from_dttm) IN (6, 7) THEN 'weekend'
            ELSE 'weekday'
        END AS day_type,
        
        period_duration_minutes,
        
        -- Метрики интенсивности
        forecast_intensity,
        actual_intensity,
        forecast_error_gco2_kwh,
        forecast_error_percent,
        
        -- Категоризация интенсивности (с использованием переменных из dbt_project.yml)
        CASE 
            WHEN forecast_intensity >= {{ var('very_high_intensity_threshold') }} THEN 'very_high'
            WHEN forecast_intensity >= {{ var('high_intensity_threshold') }} THEN 'high'
            WHEN forecast_intensity >= 150 THEN 'moderate'
            WHEN forecast_intensity >= 100 THEN 'low'
            ELSE 'very_low'
        END AS forecast_intensity_category,
        
        CASE 
            WHEN actual_intensity >= {{ var('very_high_intensity_threshold') }} THEN 'very_high'
            WHEN actual_intensity >= {{ var('high_intensity_threshold') }} THEN 'high'
            WHEN actual_intensity >= 150 THEN 'moderate'
            WHEN actual_intensity >= 100 THEN 'low'
            WHEN actual_intensity IS NOT NULL THEN 'very_low'
            ELSE NULL
        END AS actual_intensity_category,
        
        -- Оригинальные индексы из источника
        index_forecast,
        index_forecast_numeric,
        index_actual,
        index_actual_numeric,
        
        -- Статус и качество данных
        data_status,
        CASE 
            WHEN actual_intensity IS NULL THEN FALSE
            WHEN ABS(forecast_error_percent) <= 10 THEN TRUE
            ELSE FALSE
        END AS is_forecast_accurate,
        
        -- Метаданные
        valid_from_dttm,
        processed_dttm AS source_processed_dttm,
        dbt_updated_at
        
    FROM staging_data
)

SELECT * FROM enriched_data
