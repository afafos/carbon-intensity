# DBT Project: Carbon Intensity Data Warehouse

> **Аналитическое хранилище данных для carbon intensity UK энергосистемы**

## О проекте

Проект реализует DWH для данных о carbon intensity электроэнергии в UK.

### Ключевые особенности

**4-слойная архитектура**: STG -> ODS -> DWH -> DM  
**2 типа инкрементальных загрузок**: DELETE+INSERT и APPEND  
**Оконные функции и CTE** для комплексной аналитики  
**Jinja-шаблоны** во всех моделях  
**Комплексное тестирование**: 4 типа dbt-core тестов + 6 типов elementary-data тестов  
**Документация** всех моделей в schema.yml  
**Теги для разделения** моделей по функциональности

### Технологии

- **DBT 1.7.4** - трансформация данных
- **PostgreSQL** - аналитическая БД
- **Elementary Data** - мониторинг качества данных
- **dbt-utils, dbt-expectations** - расширенные тесты

---

## Архитектура слоев

```
┌─────────────────────────────────────────────────────────────────┐
│                         RAW LAYER                               │
│                    (Source: PostgreSQL)                         │
│                                                                 │
│     raw.carbon_intensity - сырые данные с версионностью SCD2    │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      STAGING LAYER (STG)                        │
│                    Materialization: VIEW                        │
│                    Tags: staging, stg                           │
│                                                                 │
│  stg_carbon_intensity_current - текущие версии данных           │
│  stg_carbon_intensity_history - вся история версий              │
│                                                                 │
│  Функции:                                                       │
│   Базовая очистка и нормализация                                │
│   Парсинг и типизация данных                                    │
│   Расчет базовых метрик                                         │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                   OPERATIONAL DATA STORE (ODS)                  │
│              Materialization: INCREMENTAL                       │
│              Strategy: DELETE+INSERT, MERGE                     │
│              Tags: ods                                          │
│                                                                 │
│  ods_carbon_intensity - оперативные данные                      │
│  ods_carbon_intensity_daily_summary - дневные сводки            │
│                                                                 │
│  Функции:                                                       │
│   Инкрементальная загрузка (DELETE+INSERT, MERGE)               │
│   Обогащение временными атрибутами                              │
│   Категоризация и бизнес-правила                                │
│   Агрегация на уровне дня                                       │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    DATA WAREHOUSE (DWH)                         │
│              Materialization: INCREMENTAL                       │
│              Strategy: APPEND                                   │
│              Tags: dwh                                          │
│                                                                 │
│  dwh_carbon_intensity_fact - факты с версионностью              │
│  dwh_forecast_accuracy     - метрики точности прогнозов         │
│                                                                 │
│  Функции:                                                       │
│  Накопление истории (immutable facts)                           │
│  Оконные функции для трендов и аналитики                        │
│  Расчет скользящих средних и перцентилей                        │
│  Анализ качества прогнозов (MAE, RMSE, MAPE)                    │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                      DATA MARTS (DM)                            │
│              Materialization: TABLE                             │
│              Tags: marts, dm                                    |
│                                                                 │
│  dm_carbon_intensity_analytics - основная витрина               │
│  dm_carbon_intensity_daily_report - ежедневные отчеты           │
│                                                                 │
│  Функции:                                                       │
│   Бизнес-метрики и KPI                                          │
│   Рекомендации по энергопотреблению                             │
│   Сравнение с историческими данными                             │
│   Детекция аномалий                                             │
│   Готовые данные для BI-инструментов                            │
└─────────────────────────────────────────────────────────────────┘
```

---

## Структура проекта

```
dbt_project/
│
├── dbt_project.yml           # Конфигурация проекта
├── profiles.yml              # Профили подключения к БД
├── packages.yml              # Зависимости (dbt_utils, elementary, etc.)
├── README.md                 # Этот файл
│
├── models/                   # SQL модели
│   ├── sources.yml          # Определение источников данных
│   │
│   ├── staging/             # STG слой
│   │   ├── schema.yml      # Документация и тесты
│   │   ├── stg_carbon_intensity_current.sql
│   │   └── stg_carbon_intensity_history.sql
│   │
│   ├── ods/                 # ODS слой
│   │   ├── schema.yml
│   │   ├── ods_carbon_intensity.sql
│   │   └── ods_carbon_intensity_daily_summary.sql
│   │
│   ├── dwh/                 # DWH слой
│   │   ├── schema.yml
│   │   ├── dwh_carbon_intensity_fact.sql
│   │   └── dwh_forecast_accuracy.sql
│   │
│   └── marts/               # DM слой
│       ├── schema.yml
│       ├── dm_carbon_intensity_analytics.sql
│       └── dm_carbon_intensity_daily_report.sql
│
└── tests/                   # Пользовательские тесты
    ├── singular/            # Специфичные тесты
    │   ├── test_forecast_vs_actual_logic.sql
    │   └── test_incremental_data_freshness.sql
    │
    └── generic/            # Переиспользуемые тесты
        └── test_valid_time_range.sql
```

---

## Использование

### Базовые команды

```bash
# Запустить все модели
dbt run

# Запустить конкретную модель
dbt run --select stg_carbon_intensity_current

# Запустить модели по тегу
dbt run --select tag:staging
dbt run --select tag:ods
dbt run --select tag:dwh
dbt run --select tag:marts

# Запустить модели и всех их потомков
dbt run --select stg_carbon_intensity_current+

# Запустить только измененные модели
dbt run --select state:modified+

# Полная перестройка (игнорируя инкрементальность)
dbt run --full-refresh
```

### Тестирование

```bash
# Запустить все тесты
dbt test

# Тесты для конкретной модели
dbt test --select ods_carbon_intensity

# Тесты по тегу
dbt test --select tag:ods

# Запустить только singular тесты
dbt test --select test_type:singular

# Запустить только generic тесты
dbt test --select test_type:generic
```

### Документация

```bash
# Генерация документации
dbt docs generate

# Запуск веб-сервера с документацией
dbt docs serve --port 8080
```

### Elementary мониторинг

```bash
# Запуск Elementary для мониторинга качества данных
edr monitor

# Генерация отчета Elementary
edr report
```

---

## Инкрементальные загрузки

В проекте используются 2 стратегии инкрементальных загрузок:

### 1. DELETE+INSERT (в ODS слое)

**Модель:** `ods_carbon_intensity`

```sql
{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='delete+insert'
    )
}}
```

**Как работает:**
1. Удаляет записи с существующими ключами
2. Вставляет новые/обновленные записи

### 2. APPEND (в DWH слое)

**Модель:** `dwh_carbon_intensity_fact`

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}
```

**Как работает:**
1. Только добавляет новые записи
2. Не обновляет существующие (immutable facts)
3. Подходит для исторических данных

### 3. MERGE (в ODS слое для агрегатов)

**Модель:** `ods_carbon_intensity_daily_summary`

```sql
{{
    config(
        materialized='incremental',
        unique_key='data_date',
        incremental_strategy='merge'
    )
}}
```

**Как работает:**
1. Обновляет существующие записи при совпадении ключа
2. Вставляет новые записи
3. Подходит для агрегатов, которые могут дополняться

---

## Тестирование

### Типы тестов в проекте

#### 1. **DBT-Core тесты**:

- **unique** - уникальность значений
- **not_null** - отсутствие NULL
- **accepted_values** - допустимые значения
- **relationships** - внешние ключи

#### 2. **Elementary тесты**:

- **volume_anomalies** - аномалии объема данных
- **freshness_anomalies** - аномалии свежести
- **dimension_anomalies** - аномалии в категориях
- **all_columns_anomalies** - аномалии во всех колонках
- **column_anomalies** - аномалии в конкретной колонке
- **event_freshness_anomalies** - аномалии свежести событий

#### 3. **dbt_utils тесты**:

- `expression_is_true` - проверка SQL выражений
- `accepted_range` - диапазоны значений
- `sequential_values` - последовательность значений

#### 4. **Custom тесты**:

- **Generic:** `test_valid_time_range` - проверка временных диапазонов
- **Singular:** `test_forecast_vs_actual_logic` - бизнес-логика прогнозов
- **Singular:** `test_incremental_data_freshness` - проверка разрывов данных

---

## Документация моделей

Все модели документированы в файлах `schema.yml`.

---

## Jinja-шаблоны

**Все модели используют Jinja-шаблоны** для:

1. **Параметризация через переменные:**
```sql
WHERE forecast_intensity > {{ var('high_intensity_threshold') }}
```

2. **Условная логика:**
```sql
{% if is_incremental() %}
  WHERE processed_dttm > (SELECT MAX(dbt_updated_at) FROM {{ this }})
{% endif %}
```

3. **Циклы для генерации кода:**
```sql
{% for cat in ['very_low', 'low', 'moderate', 'high', 'very_high'] %}
  COUNT(*) FILTER (WHERE category = '{{ cat }}') AS {{ cat }}_count{{ ',' if not loop.last else '' }}
{% endfor %}
```

4. **Макросы dbt_utils:**
```sql
{{ dbt_utils.generate_surrogate_key(['data_from_dttm', 'data_to_dttm']) }}
{{ dbt_utils.datediff('start_date', 'end_date', 'day') }}
```

---

## Оконные функции и CTE

Используем оконки и CTE для аналитики:

### Примеры оконных функций:

```sql
-- Скользящее среднее
AVG(forecast_intensity) OVER (
    ORDER BY data_from_dttm
    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
) AS ma12

-- Ранжирование
RANK() OVER (
    PARTITION BY data_date
    ORDER BY forecast_intensity DESC
) AS daily_rank

-- Сравнение с предыдущим
LAG(forecast_intensity) OVER (
    ORDER BY data_from_dttm
) AS prev_value
```

### Примеры CTE:

```sql
WITH base_data AS (
    SELECT * FROM source_table
),
enriched_data AS (
    SELECT *, calculated_field FROM base_data
),
final AS (
    SELECT * FROM enriched_data WHERE condition
)
SELECT * FROM final
```

---

## Метрики

### Основные бизнес-метрики:

- **Интенсивность CO2** (gCO₂/kWh) - текущая, средняя, медианная
- **Тренды** - краткосрочные, среднесрочные, долгосрочные
- **Точность прогнозов** - MAE, RMSE, MAPE, accuracy rate
- **Аномалии** - статистические выбросы, необычные паттерны
- **Пиковые часы** - периоды максимальной/минимальной интенсивности
- **Рекомендации** - оптимальные периоды для энергопотребления

---

## Теги моделей

Модели разделены тегами для удобства управления:

| Тег | Описание | Примеры моделей |
|-----|----------|-----------------|
| `staging`, `stg` | Staging слой | `stg_carbon_intensity_*` |
| `ods` | ODS слой | `ods_carbon_intensity*` |
| `dwh` | DWH слой | `dwh_carbon_intensity_fact` |
| `marts`, `dm` | Data Marts | `dm_carbon_intensity_*` |
| `incremental` | Инкрементальные модели | Все ODS и DWH |
| `analytics` | Аналитические витрины | `dm_*_analytics` |

**Использование:**
```bash
dbt run --select tag:staging
dbt test --select tag:ods
```
