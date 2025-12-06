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

## Система

```
┌─────────────────────┐
│  Carbon Intensity   │ <- Источник: UK энергосистема
│       API           │    Обновление: каждые 30 минут
└──────────┬──────────┘
           │ HTTP GET
           ▼
┌─────────────────────┐
│  Data Collector     │ <- Python приложение
│  (Python Service)   │    Запускается каждые 30 минут
└──────────┬──────────┘    Действие забирает данные из API
           │
           ▼
┌─────────────────────┐
│     MongoDB         │ <- Хранит сырые данные
│  (Source Database)  │    Коллекция carbon_intensity_data
└──────────┬──────────┘    
           │
           ▼
┌─────────────────────┐
│      Airflow        │ <- Оркестратор
│   (Orchestrator)    │    Запускается каждый час
└──────────┬──────────┘    EL процесс
           │
           ▼
┌─────────────────────┐
│    PostgreSQL       │ <- SQL база
│  (Analytics DB)     │    Таблица: raw.carbon_intensity
└─────────────────────┘    
```

### Процессы

**1. Сбор данных** (каждые 30 минут):
- Data Collector делает запрос к Carbon Intensity API
- Получает JSON с прогнозом на 24 часа
- Сохраняет в MongoDB

**2. EL процесс** (каждый час):
- Airflow извлекает новые данные из MongoDB
- Трансформирует формат (JSON -> SQL)
- Загружает в PostgreSQL
- Валидирует успешность переноса

**3. Мониторинг** (каждые 30 минут):
- Проверяет доступность API
- Проверяет работу Data Collector
- Собирает статистику по данным

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

# Должно показать 5 контейнеров в статусе "Up":
# - carbon_mongodb
# - carbon_postgres
# - carbon_airflow_webserver
# - carbon_airflow_scheduler
# - carbon_data_collector
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

В Airflow UI:

1. Активировать DAG **`generate_data_load`**
2. Активировать DAG **`el_mongo_to_postgres`**

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

Можно настроить в файле `.env`:

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
