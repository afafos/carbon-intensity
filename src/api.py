"""FastAPI HTTP сервис для сбора данных о carbon intensity."""

import logging
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

from carbon_api import CarbonIntensityAPI
from config import config
from database import MongoDBClient
from fastapi import FastAPI, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)

# Создание FastAPI приложения
app = FastAPI(
    title="Carbon Intensity Data Collection API",
    description="""
    API для сбора и управления данными о carbon intensity (углеродном следе) 
    электроэнергии энергосистемы Великобритании.
    
    ## Возможности
    
    * **Сбор данных** из Carbon Intensity API
    * **Сохранение** в MongoDB
    * **Статистика** по собранным данным
    * **Healthcheck** сервиса
    
    ## Источник данных
    
    Carbon Intensity API от National Grid ESO (UK)
    - URL: https://api.carbonintensity.org.uk
    - Данные обновляются каждые 30 минут
    """,
)

# Глобальные клиенты (инициализируются при старте)
db_client: Optional[MongoDBClient] = None
api_client: Optional[CarbonIntensityAPI] = None


# Pydantic модели для API
class IntensityData(BaseModel):
    """Модель данных об интенсивности CO2."""

    forecast: Optional[int] = Field(None, description="Прогнозная интенсивность CO₂ (gCO₂/kWh)")
    actual: Optional[int] = Field(None, description="Фактическая интенсивность CO₂ (gCO₂/kWh)")
    index: Optional[str] = Field(None, description="Категория (very low/low/moderate/high/very high)")


class CarbonIntensityRecord(BaseModel):
    """Модель записи о carbon intensity."""

    from_time: str = Field(..., alias="from", description="Начало временного интервала (ISO 8601)")
    to_time: str = Field(..., alias="to", description="Конец временного интервала (ISO 8601)")
    intensity: IntensityData = Field(..., description="Данные об интенсивности CO₂")

    class Config:
        """Конфигурация модели."""

        populate_by_name = True
        json_schema_extra = {
            "example": {
                "from": "2025-12-21T10:00Z",
                "to": "2025-12-21T10:30Z",
                "intensity": {
                    "forecast": 250,
                    "actual": 245,
                    "index": "moderate"
                }
            }
        }


class CollectionResponse(BaseModel):
    """Модель ответа при сборе данных."""

    success: bool = Field(..., description="Успешность операции")
    message: str = Field(..., description="Сообщение о результате")
    records_collected: int = Field(..., description="Количество собранных записей")
    records_saved: int = Field(..., description="Количество сохраненных записей")
    timestamp: str = Field(..., description="Время выполнения операции (ISO 8601)")


class StatisticsResponse(BaseModel):
    """Модель ответа со статистикой."""

    total_records: int = Field(..., description="Общее количество записей в БД")
    latest_record_time: Optional[str] = Field(None, description="Время последней записи (ISO 8601)")
    timestamp: str = Field(..., description="Время запроса статистики (ISO 8601)")


class HealthResponse(BaseModel):
    """Модель ответа healthcheck."""

    status: str = Field(..., description="Статус сервиса (healthy/unhealthy)")
    mongodb_connected: bool = Field(..., description="Статус подключения к MongoDB")
    api_available: bool = Field(..., description="Доступность Carbon Intensity API")
    timestamp: str = Field(..., description="Время проверки (ISO 8601)")


# События жизненного цикла приложения
@app.on_event("startup")
async def startup_event():
    """Инициализация при запуске приложения."""
    global db_client, api_client
    
    logger.info("Запуск Carbon Intensity API Service")
    logger.info(f"Конфигурация: {config}")
    
    try:
        # Инициализация MongoDB клиента
        db_client = MongoDBClient()
        logger.info("MongoDB клиент успешно инициализирован")
        
        # Инициализация API клиента
        api_client = CarbonIntensityAPI()
        logger.info("Carbon Intensity API клиент успешно инициализирован")
        
    except Exception as e:
        logger.error(f"Ошибка при инициализации: {e}")
        raise


@app.on_event("shutdown")
async def shutdown_event():
    """Очистка ресурсов при остановке приложения."""
    global db_client
    
    logger.info("Остановка Carbon Intensity API Service")
    
    if db_client:
        db_client.close()
        logger.info("MongoDB соединение закрыто")


# API эндпоинты
@app.get(
    "/",
    summary="Корневой эндпоинт",
    description="Возвращает информацию о сервисе",
    tags=["General"]
)
async def root():
    """Корневой эндпоинт API."""
    return {
        "service": "Carbon Intensity Data Collection API",
        "version": "1.0.0",
        "status": "running",
        "docs": "/docs",
        "openapi": "/openapi.json"
    }


@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Проверка работоспособности",
    description="Проверяет статус сервиса, подключения к MongoDB и доступность внешнего API",
    tags=["Health"],
    responses={
        200: {
            "description": "Сервис работает нормально",
            "content": {
                "application/json": {
                    "example": {
                        "status": "healthy",
                        "mongodb_connected": True,
                        "api_available": True,
                        "timestamp": "2025-12-21T10:00:00Z"
                    }
                }
            }
        }
    }
)
async def health_check():
    """Проверка работоспособности сервиса."""
    mongodb_ok = False
    api_ok = False
    
    # Проверка MongoDB
    try:
        if db_client and db_client.client:
            db_client.client.admin.command("ping")
            mongodb_ok = True
    except Exception as e:
        logger.warning(f"MongoDB healthcheck failed: {e}")
    
    # Проверка Carbon Intensity API
    try:
        if api_client:
            response = api_client._make_request("/intensity")
            api_ok = response is not None
    except Exception as e:
        logger.warning(f"API healthcheck failed: {e}")
    
    status_ok = mongodb_ok and api_ok
    
    return HealthResponse(
        status="healthy" if status_ok else "unhealthy",
        mongodb_connected=mongodb_ok,
        api_available=api_ok,
        timestamp=datetime.utcnow().isoformat() + "Z"
    )


@app.post(
    "/collect",
    response_model=CollectionResponse,
    summary="Собрать данные о carbon intensity",
    description="""
    Выполняет сбор данных из Carbon Intensity API и сохраняет их в MongoDB.
    
    Процесс:
    1. Запрашивает данные за текущие сутки (48 интервалов по 30 минут)
    2. Обрабатывает и валидирует данные
    3. Сохраняет в MongoDB с защитой от дубликатов (upsert)
    """,
    tags=["Data Collection"],
    responses={
        200: {
            "description": "Данные успешно собраны",
            "content": {
                "application/json": {
                    "example": {
                        "success": True,
                        "message": "Данные успешно собраны и сохранены",
                        "records_collected": 48,
                        "records_saved": 48,
                        "timestamp": "2025-12-21T10:00:00Z"
                    }
                }
            }
        },
        500: {"description": "Ошибка при сборе данных"}
    }
)
async def collect_data():
    """Собрать данные из API и сохранить в MongoDB."""
    try:
        logger.info("=" * 50)
        logger.info("API запрос: начало сбора данных")
        
        # Получение данных из Carbon Intensity API
        forecast_data = api_client.get_intensity_today()
        
        if not forecast_data:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Не удалось получить данные из Carbon Intensity API"
            )
        
        logger.info(f"Получено {len(forecast_data)} записей из API")
        
        # Подготовка данных для сохранения
        records_to_insert = []
        for item in forecast_data:
            try:
                record = {
                    "from": item["from"],
                    "to": item["to"],
                    "intensity": {
                        "forecast": item["intensity"].get("forecast"),
                        "actual": item["intensity"].get("actual"),
                        "index": item["intensity"].get("index"),
                    },
                }
                records_to_insert.append(record)
            except (KeyError, TypeError) as e:
                logger.warning(f"Ошибка при обработке записи: {e}")
                continue
        
        # Сохранение в MongoDB
        if not records_to_insert:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Нет данных для сохранения после обработки"
            )
        
        inserted_count = db_client.insert_many_carbon_intensity_data(records_to_insert)
        logger.info(f"Успешно сохранено {inserted_count} записей в MongoDB")
        
        # Статистика
        total_records = db_client.get_records_count()
        logger.info(f"Всего записей в БД: {total_records}")
        
        return CollectionResponse(
            success=True,
            message="Данные успешно собраны и сохранены",
            records_collected=len(records_to_insert),
            records_saved=inserted_count,
            timestamp=datetime.utcnow().isoformat() + "Z"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Ошибка при сборе данных: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Внутренняя ошибка сервера: {str(e)}"
        )


@app.get(
    "/statistics",
    response_model=StatisticsResponse,
    summary="Получить статистику",
    description="Возвращает статистику по собранным данным в MongoDB",
    tags=["Statistics"],
    responses={
        200: {
            "description": "Статистика успешно получена",
            "content": {
                "application/json": {
                    "example": {
                        "total_records": 1234,
                        "latest_record_time": "2025-12-21T10:00:00Z",
                        "timestamp": "2025-12-21T10:30:00Z"
                    }
                }
            }
        }
    }
)
async def get_statistics():
    """Получить статистику по данным."""
    try:
        total_count = db_client.get_records_count()
        latest_record = db_client.get_max_created_at()
        
        return StatisticsResponse(
            total_records=total_count,
            latest_record_time=latest_record.isoformat() + "Z" if latest_record else None,
            timestamp=datetime.utcnow().isoformat() + "Z"
        )
        
    except Exception as e:
        logger.error(f"Ошибка при получении статистики: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Не удалось получить статистику: {str(e)}"
        )


@app.get(
    "/data/latest",
    response_model=List[Dict[str, Any]],
    summary="Получить последние записи",
    description="Возвращает последние N записей из MongoDB (по умолчанию 10)",
    tags=["Data"],
    responses={
        200: {
            "description": "Записи успешно получены",
            "content": {
                "application/json": {
                    "example": [
                        {
                            "_id": "507f1f77bcf86cd799439011",
                            "from": "2025-12-21T10:00Z",
                            "to": "2025-12-21T10:30Z",
                            "intensity": {
                                "forecast": 250,
                                "actual": 245,
                                "index": "moderate"
                            },
                            "created_at": "2025-12-21T10:05:00Z"
                        }
                    ]
                }
            }
        }
    }
)
async def get_latest_data(limit: int = 10):
    """
    Получить последние записи из MongoDB.
    
    Args:
        limit: Количество записей (по умолчанию 10, максимум 100)
    """
    try:
        # Ограничение на количество записей
        limit = min(limit, 100)
        
        records = db_client.get_latest_records(limit=limit)
        
        # Конвертация ObjectId в строку для JSON сериализации
        for record in records:
            if "_id" in record:
                record["_id"] = str(record["_id"])
            if "created_at" in record:
                record["created_at"] = record["created_at"].isoformat() + "Z"
        
        return records
        
    except Exception as e:
        logger.error(f"Ошибка при получении записей: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Не удалось получить записи: {str(e)}"
        )


# Обработчик ошибок
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Глобальный обработчик исключений."""
    logger.error(f"Необработанное исключение: {exc}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "detail": "Внутренняя ошибка сервера",
            "error": str(exc)
        }
    )


if __name__ == "__main__":
    import uvicorn
    
    # Запуск сервера
    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )

