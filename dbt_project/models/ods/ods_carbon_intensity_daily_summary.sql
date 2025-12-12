{{
    config(
        materialized='incremental',
        unique_key='data_date',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        tags=['ods', 'incremental', 'aggregated']
    )
}}

/*
    Модель: ods_carbon_intensity_daily_summary
    
    Описание:
    Ежедневная сводка данных о carbon intensity.
    Использует инкрементальную загрузку по стратегии MERGE.
    
    Инкрементальная стратегия: MERGE
    - Обновляет существующие записи при совпадении ключа
    - Вставляет новые записи
    - Подходит для агрегированных данных, которые могут дополняться
    
    Бизнес-логика:
    - Агрегация данных по дням
    - Статистика по прогнозам и фактам
    - Анализ точности прогнозов
    
    Зависимости: ods_carbon_intensity
*/

WITH daily_data AS (
    SELECT
        data_date,
        data_year,
        data_month,
        data_day,
        data_day_of_week,
        data_day_of_week_num,
        day_type,
        
        -- Статистика по прогнозам
        COUNT(*) AS total_periods,
        COUNT(DISTINCT data_hour) AS hours_with_data,
        
        MIN(forecast_intensity) AS min_forecast_intensity,
        MAX(forecast_intensity) AS max_forecast_intensity,
        AVG(forecast_intensity) AS avg_forecast_intensity,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY forecast_intensity) AS median_forecast_intensity,
        STDDEV(forecast_intensity) AS stddev_forecast_intensity,
        
        -- Статистика по фактам
        COUNT(actual_intensity) AS periods_with_actual,
        MIN(actual_intensity) AS min_actual_intensity,
        MAX(actual_intensity) AS max_actual_intensity,
        AVG(actual_intensity) AS avg_actual_intensity,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY actual_intensity) AS median_actual_intensity,
        
        -- Точность прогнозов
        AVG(ABS(forecast_error_gco2_kwh)) AS avg_absolute_error,
        AVG(forecast_error_percent) AS avg_forecast_error_percent,
        COUNT(*) FILTER (WHERE is_forecast_accurate = TRUE) AS accurate_forecasts_count,
        
        -- Распределение по категориям
        {% set categories = ['very_low', 'low', 'moderate', 'high', 'very_high'] %}
        {% for cat in categories %}
        COUNT(*) FILTER (WHERE forecast_intensity_category = '{{ cat }}') AS forecast_{{ cat }}_count,
        SUM(period_duration_minutes) FILTER (WHERE forecast_intensity_category = '{{ cat }}') AS forecast_{{ cat }}_minutes{{ ',' if not loop.last else '' }}
        {% endfor %}
        
    FROM {{ ref('ods_carbon_intensity') }}
    
    {% if is_incremental() %}
    -- Инкрементальная загрузка: пересчитываем последние N дней
    -- (на случай появления новых фактических данных для старых периодов)
    WHERE data_date >= (
        SELECT COALESCE(MAX(data_date) - INTERVAL '7 days', DATE '{{ var('start_date') }}')
        FROM {{ this }}
    )
    {% endif %}
    
    GROUP BY 
        data_date,
        data_year,
        data_month,
        data_day,
        data_day_of_week,
        data_day_of_week_num,
        day_type
),

final AS (
    SELECT
        *,
        
        -- Процент точных прогнозов
        CASE 
            WHEN periods_with_actual > 0 
            THEN ROUND((accurate_forecasts_count::NUMERIC / periods_with_actual::NUMERIC) * 100, 2)
            ELSE NULL
        END AS forecast_accuracy_percent,
        
        -- Полнота данных
        ROUND((hours_with_data::NUMERIC / 24) * 100, 2) AS data_completeness_percent,
        
        -- Волатильность (насколько менялась интенсивность в течение дня)
        CASE 
            WHEN avg_forecast_intensity > 0
            THEN ROUND((stddev_forecast_intensity / avg_forecast_intensity) * 100, 2)
            ELSE NULL
        END AS forecast_volatility_percent,
        
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM daily_data
)

SELECT * FROM final
