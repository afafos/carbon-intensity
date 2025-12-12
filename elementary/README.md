# 🔍 Elementary Data Quality Monitoring

Elementary - инструмент мониторинга качества данных для DBT проектов.

## elementary_report.html

**HTML отчет не хранится в Git**

- **Обновляется:** Автоматически при каждом запуске DBT DAG
- **Доступен:** http://localhost:8082/elementary_report.html
- **Генерируется:** Скриптом `generate_report.sh` в контейнере

**Папка `reports/` содержит:**
- `.gitkeep` - чтобы папка существовала в Git
- `elementary_report.html` - игнорируется Git

---

## Типы тестов Elementary в проекте

Проект использует 6 типов Elementary Data тестов, распределенных по слоям:

### 1. **volume_anomalies**
Обнаруживает аномалии в объеме данных.

**Используется в:**
- `stg_carbon_intensity_current`
- `stg_carbon_intensity_history`
- `ods_carbon_intensity`
- `dwh_carbon_intensity_fact`
- `dwh_forecast_accuracy`
- `dm_carbon_intensity_analytics`
- `dm_carbon_intensity_daily_report`

### 2. **freshness_anomalies**
Проверяет своевременность поступления данных.

**Используется в:**
- `ods_carbon_intensity`
- `dm_carbon_intensity_analytics`

### 3. **dimension_anomalies**
Мониторит распределение значений в категориальных столбцах.

**Используется в:**
- `ods_carbon_intensity` (dimensions: data_day_of_week, time_of_day)
- `dwh_carbon_intensity_fact` (dimensions: forecast_horizon_category)

### 4. **all_columns_anomalies**
Проверяет все столбцы на различные типы аномалий.

**Используется в:**
- `stg_carbon_intensity_current`
- `dwh_carbon_intensity_fact`

### 5. **column_anomalies**
Детальный мониторинг конкретных столбцов.

**Используется в:**
- `stg_carbon_intensity_current.forecast_intensity`

### 6. **event_freshness_anomalies**
Сравнивает время события с временем обновления.

**Используется в:**
- `ods_carbon_intensity_daily_summary`
- `dm_carbon_intensity_daily_report`

## Старт

### Автоматическая генерация отчета

Отчет генерируется автоматически при каждом запуске DBT DAG в Airflow.

**Проверить обновление вручную:**
```bash
# Из корня проекта
./update_elementary_report.sh
```

### Просмотр отчета

После генерации отчет будет доступен по адресу:
- **URL:** http://localhost:8082/elementary_report.html

## Структура

```
elementary/
├── Dockerfile                 # Docker образ с elementary-data
├── generate_report.sh         # Скрипт генерации отчета
└── reports/
    ├── .gitkeep               # Для Git
    └── elementary_report.html # Отчет (после генерации)
```

## Автоматическое обновление отчета

Elementary отчет обновляется **автоматически**:

1. **При каждом запуске DBT DAG** в Airflow
2. **Задача:** `generate_elementary_report` в DAG `dbt_transformation`
3. **Скрипт:** `update_elementary_report.sh` монтируется в Airflow контейнер
