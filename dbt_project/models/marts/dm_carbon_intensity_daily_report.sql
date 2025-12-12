{{
    config(
        materialized='table',
        tags=['marts', 'dm', 'daily', 'report']
    )
}}

/*
    Модель: dm_carbon_intensity_daily_report
    
    Описание:
    Ежедневный отчет по carbon intensity для бизнес-пользователей.
    Агрегированные метрики и инсайты на уровне дня.
    
    Бизнес-кейсы:
    - Ежедневные дашборды и отчеты
    - Трекинг KPI по carbon intensity
    - Анализ паттернов по дням недели
    - Планирование энергопотребления
    
    Зависимости: dm_carbon_intensity_analytics
*/

WITH daily_base AS (
    SELECT
        data_date,
        data_year,
        data_month,
        data_day,
        data_day_of_week,
        day_type,
        
        -- Подсчет периодов
        COUNT(*) AS total_periods,
        COUNT(DISTINCT data_hour) AS hours_with_data,
        
        -- Статистика по прогнозной интенсивности
        MIN(forecast_co2_gco2_kwh) AS min_forecast,
        MAX(forecast_co2_gco2_kwh) AS max_forecast,
        AVG(forecast_co2_gco2_kwh) AS avg_forecast,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY forecast_co2_gco2_kwh) AS median_forecast,
        STDDEV(forecast_co2_gco2_kwh) AS stddev_forecast,
        
        -- Статистика по фактической интенсивности
        MIN(actual_co2_gco2_kwh) AS min_actual,
        MAX(actual_co2_gco2_kwh) AS max_actual,
        AVG(actual_co2_gco2_kwh) AS avg_actual,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY actual_co2_gco2_kwh) AS median_actual,
        
        -- Точность прогнозов
        AVG(ABS(forecast_error)) AS avg_absolute_error,
        AVG(forecast_error_percent) AS avg_error_percent,
        AVG(rolling_accuracy_rate_pct) AS avg_accuracy_rate,
        
        -- Распределение по категориям (используем Jinja для генерации)
        {% set categories = ['very_low', 'low', 'moderate', 'high', 'very_high'] %}
        {% for cat in categories %}
        COUNT(*) FILTER (WHERE forecast_category = '{{ cat }}') AS periods_{{ cat }},
        ROUND(AVG(forecast_co2_gco2_kwh) FILTER (WHERE forecast_category = '{{ cat }}'), 2) AS avg_intensity_{{ cat }}{{ ',' if not loop.last else '' }}
        {% endfor %}
        
    FROM {{ ref('dm_carbon_intensity_analytics') }}
    WHERE data_date >= CURRENT_DATE - INTERVAL '90 days'
    GROUP BY 
        data_date,
        data_year,
        data_month,
        data_day,
        data_day_of_week,
        day_type
),

/*
    Анализ временных паттернов
*/
peak_analysis AS (
    SELECT
        data_date,
        
        -- Определение пиковых часов (топ-3 по интенсивности)
        STRING_AGG(
            data_hour::TEXT || 'ч (' || forecast_co2_gco2_kwh || ' gCO₂/kWh)',
            ', '
            ORDER BY forecast_co2_gco2_kwh DESC
        ) FILTER (
            WHERE rnk <= 3
        ) AS top_3_peak_hours,
        
        -- Определение лучших часов (топ-3 самые низкие значения)
        STRING_AGG(
            data_hour::TEXT || 'ч (' || forecast_co2_gco2_kwh || ' gCO₂/kWh)',
            ', '
            ORDER BY forecast_co2_gco2_kwh ASC
        ) FILTER (
            WHERE rnk_low <= 3
        ) AS top_3_best_hours,
        
        -- Средняя интенсивность в пиковые периоды
        AVG(forecast_co2_gco2_kwh) FILTER (WHERE is_peak_carbon_period = TRUE) AS avg_peak_intensity,
        
        -- Количество пиковых периодов
        COUNT(*) FILTER (WHERE is_peak_carbon_period = TRUE) AS peak_periods_count,
        
        -- Аномалии
        COUNT(*) FILTER (WHERE is_statistical_anomaly = TRUE) AS anomaly_count
        
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY data_date ORDER BY forecast_co2_gco2_kwh DESC) AS rnk,
            ROW_NUMBER() OVER (PARTITION BY data_date ORDER BY forecast_co2_gco2_kwh ASC) AS rnk_low
        FROM {{ ref('dm_carbon_intensity_analytics') }}
        WHERE data_date >= CURRENT_DATE - INTERVAL '90 days'
    ) ranked
    GROUP BY data_date
),

/*
    Сравнение с предыдущими периодами
*/
comparative_analysis AS (
    SELECT
        data_date,
        avg_forecast,
        
        -- Сравнение с предыдущим днем
        LAG(avg_forecast, 1) OVER (ORDER BY data_date) AS prev_day_avg,
        avg_forecast - LAG(avg_forecast, 1) OVER (ORDER BY data_date) AS day_over_day_change,
        
        -- Сравнение с неделей назад (тот же день недели)
        LAG(avg_forecast, 7) OVER (ORDER BY data_date) AS prev_week_same_day_avg,
        avg_forecast - LAG(avg_forecast, 7) OVER (ORDER BY data_date) AS week_over_week_change,
        
        -- Скользящая средняя за 7 дней
        AVG(avg_forecast) OVER (
            ORDER BY data_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS ma7_forecast,
        
        -- Скользящая средняя за 30 дней
        AVG(avg_forecast) OVER (
            ORDER BY data_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS ma30_forecast
        
    FROM daily_base
),

/*
    Финальная витрина с комплексными инсайтами
*/
final_report AS (
    SELECT
        -- Идентификация даты
        b.data_date,
        b.data_year,
        b.data_month,
        b.data_day,
        b.data_day_of_week,
        b.day_type,
        TO_CHAR(b.data_date, 'DD.MM.YYYY') AS date_formatted,
        TO_CHAR(b.data_date, 'W') AS week_of_year,
        
        -- Основные метрики
        b.total_periods,
        b.hours_with_data,
        ROUND((b.hours_with_data::NUMERIC / 24) * 100, 1) AS data_completeness_pct,
        
        -- Интенсивность CO2
        ROUND(b.avg_forecast, 2) AS avg_daily_intensity,
        ROUND(b.median_forecast::NUMERIC, 2) AS median_daily_intensity,
        ROUND(b.min_forecast, 2) AS min_daily_intensity,
        ROUND(b.max_forecast, 2) AS max_daily_intensity,
        ROUND(b.stddev_forecast, 2) AS intensity_volatility,
        
        -- Распределение по категориям (используем Jinja для форматирования)
        {% for cat in ['very_low', 'low', 'moderate', 'high', 'very_high'] %}
        b.periods_{{ cat }},
        ROUND((b.periods_{{ cat }}::NUMERIC / NULLIF(b.total_periods, 0)) * 100, 1) AS pct_{{ cat }},
        {% endfor %}
        
        -- Точность прогнозов
        ROUND(b.avg_absolute_error, 2) AS avg_forecast_error,
        ROUND(b.avg_error_percent, 2) AS avg_forecast_error_pct,
        ROUND(b.avg_accuracy_rate, 1) AS forecast_accuracy_rate,
        
        -- Категория качества прогнозов за день (используем переменные и Jinja)
        {% set quality_thresholds = [
            (5, 'Отличное'),
            (10, 'Хорошее'),
            (20, 'Приемлемое'),
            (9999, 'Требует улучшения')
        ] %}
        CASE 
            {% for threshold, quality in quality_thresholds %}
            WHEN b.avg_absolute_error <= {{ threshold }} THEN '{{ quality }}'
            {% endfor %}
        END AS daily_forecast_quality,
        
        -- Пиковый анализ
        p.top_3_peak_hours,
        p.top_3_best_hours,
        ROUND(p.avg_peak_intensity, 2) AS avg_peak_intensity,
        p.peak_periods_count,
        p.anomaly_count,
        
        -- Сравнительный анализ
        ROUND(c.prev_day_avg, 2) AS prev_day_avg,
        ROUND(c.day_over_day_change, 2) AS day_over_day_change,
        ROUND(c.prev_week_same_day_avg, 2) AS prev_week_avg,
        ROUND(c.week_over_week_change, 2) AS week_over_week_change,
        ROUND(c.ma7_forecast, 2) AS ma7_intensity,
        ROUND(c.ma30_forecast, 2) AS ma30_intensity,
        
        -- Тренд (на основе скользящих средних)
        CASE 
            WHEN b.avg_forecast > c.ma7_forecast * 1.05 THEN 'Растущий'
            WHEN b.avg_forecast < c.ma7_forecast * 0.95 THEN 'Снижающийся'
            ELSE 'Стабильный'
        END AS short_term_trend,
        
        -- Рекомендация дня
        CASE 
            WHEN b.avg_forecast < 150 THEN 'Отличный день для энергопотребления! Низкая углеродная интенсивность.'
            WHEN b.avg_forecast < 200 THEN 'Хороший день для обычного энергопотребления.'
            WHEN b.avg_forecast < 250 THEN 'Умеренная углеродная интенсивность. Рассмотрите оптимизацию потребления.'
            ELSE 'Высокая углеродная интенсивность. Рекомендуется отложить энергоемкие задачи на другие дни.'
        END AS daily_recommendation,
        
        -- Метаданные
        CURRENT_TIMESTAMP AS report_generated_at
        
    FROM daily_base b
    LEFT JOIN peak_analysis p USING (data_date)
    LEFT JOIN comparative_analysis c USING (data_date)
)

SELECT * FROM final_report
ORDER BY data_date DESC
