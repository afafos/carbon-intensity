{{
    config(
        materialized='incremental',
        unique_key=['period_key', 'forecast_made_at'],
        incremental_strategy='append',
        on_schema_change='sync_all_columns',
        tags=['dwh', 'fact', 'incremental']
    )
}}

/*
    Модель: dwh_carbon_intensity_fact
    
    Описание:
    Факторная таблица DWH для хранения всей истории данных carbon intensity.
    Использует стратегию APPEND для накопления исторических данных.
    
    Инкрементальная стратегия: APPEND
    - Добавляет только новые записи
    - Не обновляет существующие
    - Подходит для неизменяемых исторических данных
    
    Бизнес-логика:
    - Хранит все версии данных (все прогнозы)
    - Анализ изменений прогнозов во времени
    - Расчет метрик качества прогнозов
    - Использование оконных функций для расчета трендов
    
    Зависимости: stg_carbon_intensity_history
*/

WITH base_data AS (
    SELECT
        -- Идентификаторы
        id AS source_id,
        {{ dbt_utils.generate_surrogate_key(['data_from_dttm', 'data_to_dttm']) }} AS period_key,
        {{ dbt_utils.generate_surrogate_key(['id', 'forecast_made_at']) }} AS fact_key,
        
        -- Временные измерения
        data_from_dttm,
        data_to_dttm,
        forecast_made_at,
        forecast_valid_until,
        forecast_horizon_hours,
        
        -- Метрики интенсивности
        forecast_intensity,
        actual_intensity,
        
        -- Индексы
        index_forecast,
        index_actual,
        
        -- Флаги версий
        is_current_version,
        is_historical_version,
        
        -- Анализ версий
        version_number,
        total_versions,
        prev_forecast_intensity,
        forecast_change_from_prev,
        
        -- Метаданные
        src_processed_dttm,
        processed_dttm
        
    FROM {{ ref('stg_carbon_intensity_history') }}
    
    {% if is_incremental() %}
    -- Инкрементальная загрузка: только новые факты
    -- Используем processed_dttm
    WHERE processed_dttm > (SELECT COALESCE(MAX(processed_dttm), TIMESTAMP '{{ var('start_date') }}') FROM {{ this }})
    {% endif %}
),

/*
    CTE с расширенной аналитикой и оконными функциями
    
    Вычисляем:
    - Тренды прогнозов (растет/падает интенсивность)
    - Скользящие средние для сглаживания
    - Ранжирование периодов по интенсивности
    - Сравнение с предыдущими периодами
*/
analytical_metrics AS (
    SELECT
        *,

        -- Скользящее среднее прогноза за последние 3 периода (1.5 часа)
        AVG(forecast_intensity) OVER (
            PARTITION BY is_current_version
            ORDER BY data_from_dttm
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS forecast_ma3,
        
        -- Скользящее среднее за последние 12 периодов (6 часов)
        AVG(forecast_intensity) OVER (
            PARTITION BY is_current_version
            ORDER BY data_from_dttm
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS forecast_ma12,
        
        -- Скользящее среднее за последние 48 периодов (24 часа)
        AVG(forecast_intensity) OVER (
            PARTITION BY is_current_version
            ORDER BY data_from_dttm
            ROWS BETWEEN 47 PRECEDING AND CURRENT ROW
        ) AS forecast_ma48,
        
        -- Изменение по сравнению с предыдущим периодом
        forecast_intensity - LAG(forecast_intensity) OVER (
            PARTITION BY is_current_version
            ORDER BY data_from_dttm
        ) AS forecast_change_from_prev_period,
        
        -- Процентное изменение
        CASE 
            WHEN LAG(forecast_intensity) OVER (
                PARTITION BY is_current_version
                ORDER BY data_from_dttm
            ) > 0
            THEN ROUND(
                ((forecast_intensity - LAG(forecast_intensity) OVER (
                    PARTITION BY is_current_version
                    ORDER BY data_from_dttm
                ))::NUMERIC / LAG(forecast_intensity) OVER (
                    PARTITION BY is_current_version
                    ORDER BY data_from_dttm
                )::NUMERIC) * 100,
                2
            )
            ELSE NULL
        END AS forecast_pct_change_from_prev_period,
        
        -- Ранг по интенсивности в течение дня
        RANK() OVER (
            PARTITION BY DATE(data_from_dttm), is_current_version
            ORDER BY forecast_intensity DESC
        ) AS daily_intensity_rank,
        
        -- Перцентиль интенсивности (0-100, где 100 = самая высокая интенсивность)
        PERCENT_RANK() OVER (
            PARTITION BY DATE(data_from_dttm), is_current_version
            ORDER BY forecast_intensity
        ) * 100 AS daily_intensity_percentile,
        
        -- Анализ качества прогнозов по горизонту
        
        -- Средняя ошибка для похожего горизонта прогнозирования
        AVG(
            CASE 
                WHEN actual_intensity IS NOT NULL 
                THEN ABS(forecast_intensity - actual_intensity)
                ELSE NULL
            END
        ) OVER (
            PARTITION BY 
                CASE 
                    WHEN forecast_horizon_hours < 1 THEN '< 1h'
                    WHEN forecast_horizon_hours < 6 THEN '1-6h'
                    WHEN forecast_horizon_hours < 12 THEN '6-12h'
                    WHEN forecast_horizon_hours < 24 THEN '12-24h'
                    ELSE '24h+'
                END
            ORDER BY forecast_made_at
            ROWS BETWEEN 100 PRECEDING AND CURRENT ROW
        ) AS avg_error_for_horizon,
        
        -- Минимум и максимум за неделю (для контекста)
        MIN(forecast_intensity) OVER (
            PARTITION BY DATE_TRUNC('week', data_from_dttm), is_current_version
        ) AS weekly_min_intensity,
        
        MAX(forecast_intensity) OVER (
            PARTITION BY DATE_TRUNC('week', data_from_dttm), is_current_version
        ) AS weekly_max_intensity,
        
        CURRENT_TIMESTAMP AS dbt_updated_at
        
    FROM base_data
),

/*
    Добавление производных метрик
*/
final AS (
    SELECT
        *,
        
        -- Категория тренда (на основе скользящих средних)
        CASE 
            WHEN forecast_ma3 > forecast_ma12 * 1.05 THEN 'rising'
            WHEN forecast_ma3 < forecast_ma12 * 0.95 THEN 'falling'
            ELSE 'stable'
        END AS short_term_trend,
        
        -- Волатильность на недельном уровне
        CASE 
            WHEN weekly_max_intensity > 0
            THEN ROUND(
                ((weekly_max_intensity - weekly_min_intensity)::NUMERIC / weekly_max_intensity::NUMERIC) * 100,
                2
            )
            ELSE NULL
        END AS weekly_volatility_pct,
        
        -- Флаг аномальных значений (выше 95-го перцентиля или ниже 5-го)
        CASE 
            WHEN daily_intensity_percentile >= 95 OR daily_intensity_percentile <= 5 
            THEN TRUE
            ELSE FALSE
        END AS is_outlier,
        
        -- Категория горизонта прогнозирования
        CASE 
            WHEN forecast_horizon_hours < 1 THEN 'immediate'  -- < 1 час
            WHEN forecast_horizon_hours < 6 THEN 'short_term'  -- 1-6 часов
            WHEN forecast_horizon_hours < 12 THEN 'medium_term'  -- 6-12 часов
            WHEN forecast_horizon_hours < 24 THEN 'long_term'  -- 12-24 часа
            ELSE 'very_long_term'  -- > 24 часов
        END AS forecast_horizon_category
        
    FROM analytical_metrics
)

SELECT * FROM final
