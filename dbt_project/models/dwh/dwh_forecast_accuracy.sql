{{
    config(
        materialized='incremental',
        unique_key='accuracy_key',
        incremental_strategy='append',
        tags=['dwh', 'metrics', 'incremental']
    )
}}

/*
    Модель: dwh_forecast_accuracy
    
    Описание:
    Метрики точности прогнозов carbon intensity для анализа качества предсказаний.
    
    Бизнес-логика:
    - Расчет ошибок прогнозирования (MAE, RMSE, MAPE)
    - Анализ точности по горизонтам прогнозирования
    - Сравнение версий прогнозов
    
    Зависимости: dwh_carbon_intensity_fact
*/

WITH base_forecasts AS (
    -- Берем только записи с фактическими значениями для расчета точности
    SELECT
        fact_key,
        period_key,
        data_from_dttm,
        data_to_dttm,
        forecast_made_at,
        forecast_horizon_hours,
        forecast_horizon_category,
        forecast_intensity,
        actual_intensity,
        version_number,
        is_current_version
    FROM {{ ref('dwh_carbon_intensity_fact') }}
    WHERE actual_intensity IS NOT NULL
    
    {% if is_incremental() %}
    -- Загружаем только новые записи, где появились факты
    AND data_from_dttm > (
        SELECT COALESCE(MAX(data_from_dttm), TIMESTAMP '{{ var('start_date') }}')
        FROM {{ this }}
    )
    {% endif %}
),

/*
    Расчет различных метрик ошибок прогнозирования
    
    Используемые метрики:
    - MAE - средняя абсолютная ошибка
    - MSE - средняя квадратичная ошибка  
    - RMSE - корень из MSE
    - MAPE - средняя абсолютная процентная ошибка
*/
accuracy_metrics AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['period_key', 'forecast_made_at']) }} AS accuracy_key,
        fact_key,
        period_key,
        
        -- Временные измерения
        data_from_dttm,
        data_to_dttm,
        forecast_made_at,
        DATE(data_from_dttm) AS data_date,
        EXTRACT(HOUR FROM data_from_dttm) AS data_hour,
        forecast_horizon_hours,
        forecast_horizon_category,
        
        -- Значения
        forecast_intensity,
        actual_intensity,
        
        -- Базовые метрики ошибок
        forecast_intensity - actual_intensity AS error,
        ABS(forecast_intensity - actual_intensity) AS absolute_error,
        POWER(forecast_intensity - actual_intensity, 2) AS squared_error,
        
        -- Процентная ошибка
        CASE 
            WHEN actual_intensity > 0 
            THEN ABS((forecast_intensity - actual_intensity)::NUMERIC / actual_intensity::NUMERIC) * 100
            ELSE NULL
        END AS absolute_percentage_error,
        
        -- Точность (обратная к ошибке, в процентах)
        CASE 
            WHEN actual_intensity > 0 
            THEN GREATEST(
                0,
                100 - (ABS((forecast_intensity - actual_intensity)::NUMERIC / actual_intensity::NUMERIC) * 100)
            )
            ELSE NULL
        END AS accuracy_percentage,
        
        -- Флаги качества прогноза
        {% set error_thresholds = [
            (5, 'excellent'),
            (10, 'good'),
            (20, 'acceptable'),
            (9999, 'poor')
        ] %}
        CASE 
            {% for threshold, quality in error_thresholds %}
            WHEN ABS(forecast_intensity - actual_intensity) <= {{ threshold }} THEN '{{ quality }}'
            {% endfor %}
        END AS forecast_quality,
        
        -- Направление ошибки
        CASE 
            WHEN forecast_intensity > actual_intensity THEN 'overestimated'
            WHEN forecast_intensity < actual_intensity THEN 'underestimated'
            ELSE 'exact'
        END AS error_direction,
        
        version_number,
        is_current_version,
        
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM base_forecasts
),

/*
    Агрегированная статистика
*/
aggregated_stats AS (
    SELECT
        *,
        
        -- Скользящая средняя ошибка за последние 24 часа (48 периодов)
        AVG(absolute_error) OVER (
            PARTITION BY forecast_horizon_category
            ORDER BY data_from_dttm
            ROWS BETWEEN 47 PRECEDING AND CURRENT ROW
        ) AS rolling_24h_mae,
        
        -- RMSE за последние 24 часа
        SQRT(AVG(squared_error) OVER (
            PARTITION BY forecast_horizon_category
            ORDER BY data_from_dttm
            ROWS BETWEEN 47 PRECEDING AND CURRENT ROW
        )) AS rolling_24h_rmse,
        
        -- Процент точных прогнозов (ошибка <= 10%) в окне 24 часа
        AVG(
            CASE WHEN absolute_percentage_error <= 10 THEN 1.0 ELSE 0.0 END
        ) OVER (
            PARTITION BY forecast_horizon_category
            ORDER BY data_from_dttm
            ROWS BETWEEN 47 PRECEDING AND CURRENT ROW
        ) * 100 AS rolling_24h_accuracy_rate,
        
        -- Средняя абсолютная ошибка за неделю
        AVG(absolute_error) OVER (
            PARTITION BY DATE_TRUNC('week', data_from_dttm), forecast_horizon_category
        ) AS weekly_avg_error,
        
        -- Улучшается ли точность со временем? (сравнение с предыдущим периодом)
        CASE 
            WHEN absolute_error < LAG(absolute_error) OVER (
                PARTITION BY forecast_horizon_category
                ORDER BY data_from_dttm
            )
            THEN TRUE
            ELSE FALSE
        END AS is_improving
        
    FROM accuracy_metrics
)

SELECT * FROM aggregated_stats
