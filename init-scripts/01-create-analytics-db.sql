CREATE DATABASE analytics;

\c analytics;

CREATE SCHEMA IF NOT EXISTS raw;

CREATE SCHEMA IF NOT EXISTS staging;

CREATE SCHEMA IF NOT EXISTS marts;

CREATE TABLE IF NOT EXISTS raw.carbon_intensity (
    id SERIAL PRIMARY KEY,
    
    -- Бизнес-ключ: период данных (к какому времени относятся данные)
    data_from_dttm TIMESTAMP NOT NULL,
    data_to_dttm TIMESTAMP NOT NULL,
    
    forecast_intensity INTEGER,
    actual_intensity INTEGER,
    index_forecast VARCHAR(20),
    index_actual VARCHAR(20),
    
    -- Версионность (SCD Type 2)
    valid_from_dttm TIMESTAMP NOT NULL,
    valid_to_dttm TIMESTAMP NOT NULL DEFAULT '5999-01-01 00:00:00',
    
    src_processed_dttm TIMESTAMP,
    processed_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(data_from_dttm, data_to_dttm, valid_from_dttm)
);

CREATE INDEX idx_carbon_intensity_data_from ON raw.carbon_intensity(data_from_dttm);
CREATE INDEX idx_carbon_intensity_data_to ON raw.carbon_intensity(data_to_dttm);
CREATE INDEX idx_carbon_intensity_valid_from ON raw.carbon_intensity(valid_from_dttm);
CREATE INDEX idx_carbon_intensity_valid_to ON raw.carbon_intensity(valid_to_dttm);
CREATE INDEX idx_carbon_intensity_processed ON raw.carbon_intensity(processed_dttm);
CREATE INDEX idx_carbon_intensity_src_processed ON raw.carbon_intensity(src_processed_dttm);

GRANT ALL PRIVILEGES ON DATABASE analytics TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA raw TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA staging TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA marts TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA raw TO airflow;

COMMENT ON TABLE raw.carbon_intensity IS 'Сырые данные о carbon intensity из UK энергосистемы с версионностью (SCD Type 2)';
COMMENT ON COLUMN raw.carbon_intensity.data_from_dttm IS 'Начало периода данных (к какому времени относятся данные)';
COMMENT ON COLUMN raw.carbon_intensity.data_to_dttm IS 'Конец периода данных';
COMMENT ON COLUMN raw.carbon_intensity.forecast_intensity IS 'Прогнозная интенсивность CO2 (gCO2/kWh)';
COMMENT ON COLUMN raw.carbon_intensity.actual_intensity IS 'Фактическая интенсивность CO2 (gCO2/kWh)';
COMMENT ON COLUMN raw.carbon_intensity.index_forecast IS 'Индекс прогнозной интенсивности (very low/low/moderate/high/very high)';
COMMENT ON COLUMN raw.carbon_intensity.index_actual IS 'Индекс фактической интенсивности';
COMMENT ON COLUMN raw.carbon_intensity.valid_from_dttm IS 'Время начала действия этой версии записи (SCD Type 2)';
COMMENT ON COLUMN raw.carbon_intensity.valid_to_dttm IS 'Время окончания действия версии (5999-01-01 для текущей версии)';
COMMENT ON COLUMN raw.carbon_intensity.src_processed_dttm IS 'Время обработки записи в источнике (MongoDB)';
COMMENT ON COLUMN raw.carbon_intensity.processed_dttm IS 'Время загрузки записи в PostgreSQL';

-- View для актуальных данных
CREATE OR REPLACE VIEW raw.carbon_intensity_current AS
SELECT 
    data_from_dttm,
    data_to_dttm,
    forecast_intensity,
    actual_intensity,
    index_forecast,
    index_actual,
    src_processed_dttm,
    processed_dttm,
    valid_from_dttm,
    valid_to_dttm
FROM raw.carbon_intensity
WHERE valid_to_dttm = '5999-01-01 00:00:00'
ORDER BY data_from_dttm;

COMMENT ON VIEW raw.carbon_intensity_current IS 'Текущие (актуальные) версии данных о carbon intensity';

-- View для анализа изменений прогнозов
CREATE OR REPLACE VIEW raw.carbon_intensity_forecast_changes AS
SELECT 
    data_from_dttm,
    data_to_dttm,
    forecast_intensity,
    actual_intensity,
    valid_from_dttm as forecast_made_at,
    valid_to_dttm as forecast_valid_until,
    EXTRACT(EPOCH FROM (data_from_dttm - valid_from_dttm))/3600 as forecast_horizon_hours,
    CASE WHEN valid_to_dttm = '5999-01-01 00:00:00' THEN TRUE ELSE FALSE END as is_current
FROM raw.carbon_intensity
ORDER BY data_from_dttm, valid_from_dttm;

COMMENT ON VIEW raw.carbon_intensity_forecast_changes IS 'История изменений прогнозов с горизонтом прогнозирования';

