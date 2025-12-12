# Carbon Intensity Data Pipeline

> Проект для сбора и обработки данных о carbon intensity (углеродном следе) электроэнергии энергосистемы Великобритании

## О проекте

### Источник данных

**Carbon Intensity API** от National Grid ESO (UK)  
- URL: https://api.carbonintensity.org.uk
- Данные обновляются каждые 30 минут

### Что собираем

- **Прогнозная интенсивность CO₂** (gCO₂/kWh)
- **Фактическая интенсивность CO₂** (gCO₂/kWh)
- **Временные ряды**: 48 интервалов по 30 минут (24 часа)

---

## Старт

### 1. Запуск всех сервисов

```bash
docker-compose up -d
```

### 2. Доступ к сервисам

| Сервис | URL | Креды |
|--------|-----|-------|
| **Airflow UI** | http://localhost:8080 | admin / admin |
| **Jupyter Lab** | http://localhost:8888/lab | без пароля |
| **Elementary Report** | http://localhost:8082/elementary_report.html | без пароля |


---

## Архитектура решения

### Высокоуровневая схема

```
┌──────────────────────────────────────────────────────────────────────────┐
│                            АРХИТЕКТУРА СИСТЕМЫ                           │
└──────────────────────────────────────────────────────────────────────────┘

                            ┌─────────────────────┐
                            │  Carbon Intensity   │
                            │       API           │ <- Источник данных
                            │  (UK National Grid) │    (каждые 30 мин)
                            └──────────┬──────────┘
                                       │ HTTP GET
                                       ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          APP: Data Collector                            │
│                         (Python Service + Docker)                       │
│                                                                         │
│  Запрос к API каждые 30 минут                                           │
│  Сохранение JSON в MongoDB                                              │
│  Логирование и мониторинг                                               │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │ Insert
                                 ▼
                      ┌──────────────────────┐
                      │      MongoDB         │
                      │   (Source Storage)   │
                      │                      │
                      │  Сырые JSON данные   │
                      │  Upsert по периоду   │
                      └──────────┬───────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        AIRFLOW: Оркестрация                             │
│                          (Scheduler + Webserver)                        │
│                                                                         │
│  DAG 1: EL MongoDB → PostgreSQL (каждый час)                            │
│  ├─ Extract: Чтение новых данных из MongoDB                             │
│  ├─ Transform: Парсинг JSON, добавление версионности (SCD2)             │
│  └─ Load: Загрузка в PostgreSQL (raw.carbon_intensity)                  │
│                                                                         │
│  DAG 2: DBT Transformations (каждые 2 часа)                             │
│  ├─ dbt deps: Установка зависимостей                                    │
│  ├─ dbt run: Построение всех моделей (STG→ODS→DWH→DM)                   │
│  ├─ dbt test: Запуск тестов качества данных                             │
│  └─ Elementary: Мониторинг и отчеты                                     │
│                                                                         |
│  DAG 3: Data Generation (для тестов)                                    │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │ EL + DBT
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     POSTGRESQL: Analytics Database                      │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │ RAW LAYER: raw.carbon_intensity                                     ││
│  │ Сырые данные из MongoDB с SCD Type 2                                ││
│  └─────────────────────────────────────────────────────────────────────┘│
│                                 │                                       │
│                                 │ DBT Transformation                    │
│                                 ▼                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │ STAGING LAYER: staging.*                                            ││
│  │  stg_carbon_intensity_current  (VIEW)                               ││
│  │  stg_carbon_intensity_history  (VIEW)                               ││
│  │ Функции: Очистка, нормализация, базовые метрики                     ││
│  └─────────────────────────────────────────────────────────────────────┘│
│                                 │                                       │
│                                 ▼                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │ ODS LAYER: ods.*                                                    ││
│  │  ods_carbon_intensity (INCREMENTAL: DELETE+INSERT)                  ││
│  │  ods_carbon_intensity_daily_summary (INCREMENTAL: MERGE)            ││
│  │ Функции: Обогащение, категоризация, дневные агрегаты                ││
│  └─────────────────────────────────────────────────────────────────────┘│
│                                 │                                       │
│                                 ▼                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │ DWH LAYER: dwh.*                                                    ││
│  │  dwh_carbon_intensity_fact (INCREMENTAL: APPEND)                    ││
│  │  dwh_forecast_accuracy (INCREMENTAL: APPEND)                        ││
│  │ Функции: История, оконные функции, метрики качества                 ││
│  └─────────────────────────────────────────────────────────────────────┘│
│                                 │                                       │
│                                 ▼                                       │
│  ┌─────────────────────────────────────────────────────────────────────┐│
│  │ DATA MARTS: marts.*                                                 ││
│  │  dm_carbon_intensity_analytics (TABLE)                              ││
│  │  dm_carbon_intensity_daily_report (TABLE)                           ││
│  │ Функции: BI-метрики, рекомендации, готовые отчеты                   ││
│  └─────────────────────────────────────────────────────────────────────┘│
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    BI & ANALYTICS: Визуализация                         │
│                                                                         │
│  Jupyter Notebook: Исследовательский анализ данных                      │
│  Elementary: Отчеты качества данных                                     |
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Компоненты системы

### 1. **APP** - Data Collector (Python Service)

**Расположение:** `src/`

**Функции:**
- Сбор данных из Carbon Intensity API каждые 30 минут
- Сохранение в MongoDB с upsert стратегией
- Логирование и метрики
- Запуск в Docker контейнере

---

### 2. **AIRFLOW** - Оркестрация процессов

**Расположение:** `airflow/`

**DAGs:**

#### `el_mongo_to_postgres` (каждый час)
- Извлечение новых данных из MongoDB
- Трансформация и добавление версионности (SCD Type 2). Почему все-таки не 100% ELT - есть специфика: сложная логика версионирования. Один период времени имеет множество версий прогноза, и нужно реализовать SCD Type 2 - закрывать старые версии, открывать новые, отслеживать изменения. Поэтому тут минимально с Python делаем то, что у него лучше получается - парсинг и валидацию. А потом уже DBT с SQL используем для агрегаций, аналитики и т.п. Так проблемы данных выявляются до попадания в DWH, не засоряем raw слой. 
- Загрузка в PostgreSQL raw слой

#### `dbt_transformation` (каждые 2 часа)
- Запуск DBT моделей
- Тестирование качества данных
- Elementary мониторинг

#### `generate_data_load` (для разработки)
- Генерация тестовых данных

---

### 3. **DWH** - Data Warehouse (PostgreSQL + DBT)

**Расположение:** `dbt_project/`

**Слои:**

#### **STG** (Staging) - Views
- Очистка и нормализация
- Базовые трансформации
- Модели: `stg_carbon_intensity_current`, `stg_carbon_intensity_history`

#### **ODS** (Operational Data Store) - Incremental (DELETE+INSERT, MERGE)
- Оперативные данные с обогащением
- Инкрементальная загрузка
- Модели: `ods_carbon_intensity`, `ods_carbon_intensity_daily_summary`

#### **DWH** (Data Warehouse) - Incremental (APPEND)
- Исторические факты
- Оконные функции и аналитика
- Модели: `dwh_carbon_intensity_fact`, `dwh_forecast_accuracy`

#### **DM** (Data Marts) - Tables
- Бизнес-витрины
- Готовые метрики для BI
- Модели: `dm_carbon_intensity_analytics`, `dm_carbon_intensity_daily_report`

---

### 4. **DBT_PROJECT** - Трансформации данных

**Особенности:**

**4-слойная архитектура**: STG -> ODS -> DWH -> DM  
**2 типа инкрементальных загрузок**: DELETE+INSERT, APPEND, MERGE  
**Оконные функции**: скользящие средние, ранжирование, перцентили  
**CTE**: многоуровневые запросы для сложной логики  
**Jinja-шаблоны**: параметризация, циклы, условная логика  
**Комплексное тестирование**: 
  - 4 типа dbt-core тестов (unique, not_null, accepted_values, relationships)
  - 6 типов elementary-data тестов
  - Custom generic и singular тесты  
**Документация**: schema.yml для всех моделей  
**Теги**: для разделения моделей по функциональности

**См. подробнее:** `dbt_project/README.md`

---

### Процессы

#### **1. Сбор данных** (каждые 30 минут)
```
Carbon API → Data Collector → MongoDB
```
- Запрос к API
- Парсинг JSON
- Upsert в MongoDB

#### **2. EL процесс** (каждый час)
```
MongoDB → Airflow → PostgreSQL (raw layer)
```
- Извлечение новых данных
- Добавление версионности (SCD Type 2)
- Загрузка в raw.carbon_intensity

#### **3. DBT трансформации** (каждые 2 часа)
```
PostgreSQL raw → DBT → PostgreSQL (staging/ods/dwh/marts)
```
- Построение моделей по слоям
- Инкрементальные загрузки
- Тесты качества

#### **4. BI и визуализация**
```
Jupyter
```
- Аналитика и инсайты
- Отчеты

---

## Данные

### Что мы собираем за один запрос

**API endpoint:** `/intensity/date` (данные за текущие сутки)

**Количество записей:** 48 интервалов  
**Длительность интервала:** 30 минут  
**Покрытие:** 24 часа (от 00:00 до 23:59)

**Пример:**
```
00:00-00:30  запись 1
00:30-01:00  запись 2
01:00-01:30  запись 3
...
23:30-00:00  запись 48
```

### Что содержит каждая запись

```json
{
  "from": "2025-12-06T10:00Z",      // Начало интервала
  "to": "2025-12-06T10:30Z",        // Конец интервала
  "intensity": {
    "forecast": 250,                // Прогноз CO₂ (gCO₂/kWh)
    "actual": 245,                  // Факт CO₂ (gCO₂/kWh)
    "index": "moderate"             // Категория (very low/low/moderate/high/very high)
  }
}
```

### Как накапливаются данные

- **MongoDB**: хранит только последнюю версию для каждого интервала (upsert)
- **PostgreSQL**: хранит всю историю изменений (SCD Type 2)

**Пример версионности в PostgreSQL:**
```
Интервал 10:00-10:30:
- Версия 1 (08:00): forecast=250, actual=null
- Версия 2 (10:00): forecast=252, actual=null  (прогноз уточнился)
- Версия 3 (10:30): forecast=252, actual=245   (появился факт)
```

---

## Установка и запуск

### Сборка Docker образов

```bash
docker-compose build
```

### Запуск всех сервисов

```bash
docker-compose up -d
```

**Проверка**: Все контейнеры запущены
```bash
docker-compose ps

# Должно показать 8 контейнеров в статусе "Up":
# - carbon_mongodb           (MongoDB база данных)
# - carbon_postgres          (PostgreSQL DWH)
# - carbon_airflow_webserver (Airflow UI)
# - carbon_airflow_scheduler (Airflow планировщик)
# - carbon_data_collector    (Сбор данных из API)
# - carbon_jupyter           (Jupyter Lab для анализа)
# - carbon_elementary        (Elementary data quality)
# - carbon_nginx             (Веб-сервер для Elementary отчетов)
```

---

### Проверка Airflow

### Открыть Airflow UI

```bash
http://localhost:8080
```

**Данные для входа**:
- **Логин**: `admin`
- **Пароль**: `admin`

---

### Активация DAGs

В Airflow UI активируйте DAG:

1. **`el_mongo_to_postgres`** - перенос данных MongoDB → PostgreSQL (каждый час)
2. **`dbt_transformation`** - DBT трансформации и тесты (каждые 2 часа)
3. **`generate_data_load`** (опционально) - генерация тестовых данных

---

### Ожидание первых данных

Логи в реальном времени:
```bash
docker-compose logs -f data-collector

# Должны увидеть сообщения типа:
# "Получено N записей из API"
# "Успешно сохранено N записей в MongoDB"
```

---

## Просмотр данных

**Скриншоты примеров:** См. папку `screenshots/`

### MongoDB

```bash
# Подключиться к MongoDB
docker-compose exec mongodb mongosh -u admin -p admin123
```

В MongoDB shell:
```javascript
// Использовать базу данных
use carbon_intensity

// Количество записей
db.carbon_intensity_data.count()

// Посмотреть 2 записи
db.carbon_intensity_data.find().limit(2).pretty()

// Последние 5 записей
db.carbon_intensity_data.find().sort({created_at: -1}).limit(5).pretty()

// Выход
exit
```

**Пример документа**:
```json
  {
    _id: ObjectId('69334cbcf5f89d688f9341fd'),
    from: '2025-12-05T00:30Z',
    to: '2025-12-05T01:00Z',
    created_at: ISODate('2025-12-05T23:59:26.783Z'),
    intensity: { 
        forecast: 166, 
        actual: 154, 
        index: 'moderate' 
    }
  }
```

### PostgreSQL

```bash
# Подключиться к PostgreSQL
docker-compose exec postgres psql -U airflow -d analytics
```

В PostgreSQL shell:
```sql
-- Количество записей
SELECT COUNT(*) FROM raw.carbon_intensity;

-- Последние 5 записей
SELECT 
    from_timestamp,
    to_timestamp,
    forecast_intensity,
    actual_intensity,
    index_forecast
FROM raw.carbon_intensity
ORDER BY from_timestamp DESC
LIMIT 5;

-- Средняя интенсивность за последние 24 часа
SELECT 
    AVG(forecast_intensity) as avg_forecast,
    AVG(actual_intensity) as avg_actual
FROM raw.carbon_intensity
WHERE from_timestamp >= NOW() - INTERVAL '24 hours';

\q
```

---

### Переменные окружения

Можно настроить в файле `.env` (пример в .env-example):

```bash
# MongoDB
MONGODB_HOST=mongodb
MONGODB_PORT=27017
MONGODB_USER=admin
MONGODB_PASSWORD=admin123
MONGODB_DATABASE=carbon_intensity

# PostgreSQL
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow

# Data Collector
COLLECTION_INTERVAL=1800  # секунд (30 минут)
```

---

## Дополнительные возможности

### Jupyter Lab - Анализ данных

Jupyter Lab доступен после запуска контейнеров:

```bash
http://localhost:8888/lab
```

**Что внутри:**
- `carbon_intensity_analysis.ipynb` - готовый анализ с визуализациями
- Доступ к PostgreSQL для SQL запросов

---

### Elementary Data - Мониторинг качества данных

Elementary отчеты генерируются **автоматически** при каждом запуске DBT DAG.

**Просмотр отчета:**
```bash
http://localhost:8082/elementary_report.html
```

**Что показывает:**
- 6 типов Elementary тестов
- Графики аномалий и трендов
- Статистика качества данных
- История выполнения тестов

**Ручная генерация** (если нужно):
```bash
./update_elementary_report.sh
```

---

### Pre-commit hooks

Автоматическая проверка кода перед коммитом:

**Что проверяется:**
- Python: Black, Ruff, isort, mypy
- YAML: prettier, yamllint
- SQL: SQLFluff
- DBT: базовые тесты

---

## Структура директорий

```
carbon-intensity/
├── README.md                          # Этот файл
├── docker-compose.yml                 # Оркестрация всех сервисов
├── .env-example                       # Пример переменных окружения
├── .gitignore                         # Git исключения
├── .pre-commit-config.yaml           # Pre-commit hooks
├── .sqlfluff                         # SQL форматирование
├── pyproject.toml                    # Python настройки (Black, Ruff)
├── requirements-dev.txt              # Dev зависимости
│
├── src/                              # Приложение для сбора данных
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py
│   ├── carbon_api.py
│   ├── database.py
│   └── config.py
│
├── airflow/                          # Airflow DAGs и конфиги
│   ├── Dockerfile
│   ├── requirements.txt
│   └── dags/
│       ├── el_mongo_to_postgres.py
│       ├── dbt_transformation.py
│       └── generate_data_dag.py
│
├── dbt_project/                      # DBT проект
│   ├── README.md                     # Документация DBT
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── requirements.txt
│   ├── models/
│   │   ├── sources.yml
│   │   ├── staging/
│   │   ├── ods/
│   │   ├── dwh/
│   │   └── marts/
│   └── tests/
│       ├── singular/
│       └── generic/
│
├── notebooks/                        # Jupyter notebooks для анализа
│   ├── carbon_intensity_analysis.ipynb
│   └── requirements.txt
│
├── elementary/                       # Elementary data quality
│   ├── Dockerfile
│   ├── generate_report.sh
│   └── reports/
│       └── elementary_report.html
│
├── init-scripts/                     # SQL скрипты инициализации
│   └── 01-create-analytics-db.sql
│
├── scripts/                          # Утилиты
│   └── setup_airflow_connections.sh  # Настройка Airflow подключений
│
└── update_elementary_report.sh       # Скрипт обновления Elementary отчета
```
