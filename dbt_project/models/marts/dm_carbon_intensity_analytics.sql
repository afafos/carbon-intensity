{{
    config(
        materialized='table',
        tags=['marts', 'dm', 'analytics']
    )
}}

/*
    Модель: dm_carbon_intensity_analytics
    
    Описание:
    Аналитическая витрина для комплексного анализа carbon intensity UK энергосистемы.
    Объединяет данные из разных источников для создания единого представления.
    
    Бизнес-кейсы:
    - Анализ паттернов carbon intensity по времени суток, дням недели
    - Оценка точности прогнозирования
    - Определение пиковых и низких периодов
    - Сравнение текущих значений с историческими трендами
    
    Используемые Jinja-шаблоны:
    - Динамическая генерация категорий интенсивности
    - Параметризация через переменные проекта
    - Условная логика для разных периодов
    
    Зависимости: ods_carbon_intensity, dwh_forecast_accuracy
*/

{% set intensity_levels = ['very_low', 'low', 'moderate', 'high', 'very_high'] %}
{% set time_periods = ['morning', 'afternoon', 'evening', 'night'] %}

WITH current_data AS (
    -- Берем актуальные данные из ODS
    SELECT
        period_key,
        data_from_dttm,
        data_to_dttm,
        data_date,
        data_year,
        data_month,
        data_day,
        data_hour,
        data_day_of_week,
        data_day_of_week_num,
        time_of_day,
        day_type,
        
        forecast_intensity,
        actual_intensity,
        forecast_intensity_category,
        actual_intensity_category,
        
        forecast_error_gco2_kwh,
        forecast_error_percent,
        is_forecast_accurate,
        
        data_status
        
    FROM {{ ref('ods_carbon_intensity') }}
    WHERE data_date >= CURRENT_DATE - INTERVAL '90 days'  -- последние 3 месяца
),

accuracy_data AS (
    -- Берем метрики точности из DWH
    SELECT
        period_key,
        absolute_error,
        absolute_percentage_error,
        forecast_quality,
        error_direction,
        rolling_24h_mae,
        rolling_24h_rmse,
        rolling_24h_accuracy_rate
    FROM {{ ref('dwh_forecast_accuracy') }}
    WHERE data_date >= CURRENT_DATE - INTERVAL '90 days'
),

/*
    Объединяем данные и добавляем расширенную аналитику
*/
combined_data AS (
    SELECT
        c.*,
        a.absolute_error,
        a.absolute_percentage_error,
        a.forecast_quality,
        a.error_direction,
        a.rolling_24h_mae,
        a.rolling_24h_rmse,
        a.rolling_24h_accuracy_rate,
        
        
        -- Категоризация по пользовательским порогам из переменных
        CASE 
            WHEN c.forecast_intensity < 100 THEN 'green_energy'  -- очень чистая энергия
            WHEN c.forecast_intensity BETWEEN 100 AND {{ var('high_intensity_threshold') }} THEN 'moderate_impact'
            WHEN c.forecast_intensity > {{ var('very_high_intensity_threshold') }} THEN 'high_carbon'
            ELSE 'standard'
        END AS energy_cleanliness,
        
        -- Флаг пикового периода (высокая интенсивность в рабочее время)
        CASE 
            WHEN c.forecast_intensity > {{ var('high_intensity_threshold') }}
                AND c.time_of_day IN ('morning', 'afternoon')
                AND c.day_type = 'weekday'
            THEN TRUE
            ELSE FALSE
        END AS is_peak_carbon_period,
        
        -- Рекомендация по потреблению энергии (используем Jinja для генерации)
        {% set recommendation_rules = [
            ('very_low', 'Отличное время для энергопотребления'),
            ('low', 'Хорошее время для энергопотребления'),
            ('moderate', 'Умеренное время для энергопотребления'),
            ('high', 'По возможности отложите энергопотребление'),
            ('very_high', 'Избегайте энергопотребления')
        ] %}
        CASE c.forecast_intensity_category
            {% for level, recommendation in recommendation_rules %}
            WHEN '{{ level }}' THEN '{{ recommendation }}'
            {% endfor %}
            ELSE 'Нет данных'
        END AS consumption_recommendation
        
    FROM current_data c
    LEFT JOIN accuracy_data a USING (period_key)
),

/*
    Добавляем статистику и сравнение с историческими данными
*/
statistical_analysis AS (
    SELECT
        *,
        
        -- Сравнение с средними значениями за аналогичное время суток
        AVG(forecast_intensity) OVER (
            PARTITION BY data_hour, day_type
        ) AS avg_intensity_same_hour,
        
        STDDEV(forecast_intensity) OVER (
            PARTITION BY data_hour, day_type
        ) AS stddev_intensity_same_hour,
        
        -- Z-score (насколько текущее значение отклоняется от нормы)
        CASE 
            WHEN STDDEV(forecast_intensity) OVER (PARTITION BY data_hour, day_type) > 0
            THEN (
                forecast_intensity - AVG(forecast_intensity) OVER (PARTITION BY data_hour, day_type)
            ) / STDDEV(forecast_intensity) OVER (PARTITION BY data_hour, day_type)
            ELSE 0
        END AS intensity_z_score,
        
        -- Ранг текущего периода по интенсивности среди аналогичных часов
        DENSE_RANK() OVER (
            PARTITION BY data_hour, day_type
            ORDER BY forecast_intensity DESC
        ) AS intensity_rank_same_hour,
        
        -- Процентиль (где текущее значение относительно исторических)
        PERCENT_RANK() OVER (
            PARTITION BY data_hour, day_type
            ORDER BY forecast_intensity
        ) * 100 AS intensity_percentile_same_hour
        
    FROM combined_data
),

/*
    Вычисляем минимальную среднюю интенсивность по часам
    для расчета потенциальной экономии CO2
*/
hourly_intensity_stats AS (
    SELECT 
        MIN(avg_intensity) AS min_hourly_avg_intensity
    FROM (
        SELECT 
            data_hour,
            AVG(forecast_intensity) AS avg_intensity
        FROM combined_data
        GROUP BY data_hour
    ) hourly_avgs
),

/*
    Финальная витрина с бизнес-метриками и агрегатами
    Используем Jinja для параметризации расчетов
*/
final_mart AS (
    SELECT
        -- Идентификаторы и время
        period_key,
        data_from_dttm AS period_start,
        data_to_dttm AS period_end,
        data_date,
        data_year,
        data_month,
        data_day,
        data_hour,
        data_day_of_week,
        time_of_day,
        day_type,
        
        -- Основные метрики интенсивности
        forecast_intensity AS forecast_co2_gco2_kwh,
        actual_intensity AS actual_co2_gco2_kwh,
        forecast_intensity_category AS forecast_category,
        actual_intensity_category AS actual_category,
        
        -- Точность прогноза
        forecast_error_gco2_kwh AS forecast_error,
        forecast_error_percent,
        is_forecast_accurate,
        forecast_quality,
        error_direction,
        
        -- Метрики качества за последние 24 часа
        rolling_24h_mae AS rolling_mean_absolute_error,
        rolling_24h_rmse AS rolling_root_mean_squared_error,
        rolling_24h_accuracy_rate AS rolling_accuracy_rate_pct,
        
        -- Категории и рекомендации
        energy_cleanliness AS energy_cleanliness_level,
        is_peak_carbon_period,
        consumption_recommendation,
        
        -- Статистический анализ
        avg_intensity_same_hour AS historical_avg_for_hour,
        intensity_z_score,
        intensity_percentile_same_hour,
        
        -- Флаги аномалий
        CASE 
            WHEN ABS(intensity_z_score) > 2 THEN TRUE  -- отклонение > 2 сигм
            ELSE FALSE
        END AS is_statistical_anomaly,
        
        CASE 
            WHEN intensity_percentile_same_hour > 95 THEN 'unusually_high'
            WHEN intensity_percentile_same_hour < 5 THEN 'unusually_low'
            ELSE 'normal'
        END AS intensity_anomaly_type,
        
        -- Расчет потенциального экологического эффекта
        -- Сколько CO2 можно сэкономить, если перенести 1 кВт*ч на период с минимальной интенсивностью
        {% set typical_consumption_kwh = 1.0 %}
        CASE 
            WHEN avg_intensity_same_hour > 0
            THEN ROUND(
                (forecast_intensity - h.min_hourly_avg_intensity) * {{ typical_consumption_kwh }},
                2
            )
            ELSE NULL
        END AS potential_co2_saving_per_kwh,
        
        -- Метаданные
        data_status,
        CURRENT_TIMESTAMP AS mart_updated_at
        
    FROM statistical_analysis
    CROSS JOIN hourly_intensity_stats h
)

SELECT * FROM final_mart
ORDER BY period_start DESC
