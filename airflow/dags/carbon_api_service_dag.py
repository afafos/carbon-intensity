"""
DAG для работы с HTTP API сервисом сбора данных о carbon intensity.
Периодически вызывает HTTP эндпоинты для сбора данных и мониторинга.
"""

import logging
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# URL API сервиса (внутри Docker сети)
API_BASE_URL = "http://carbon-api:8000"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def check_api_health() -> dict:
    """
    Проверить работоспособность API сервиса.
    
    Returns:
        dict: Статус healthcheck
    """
    logger.info("Проверка работоспособности Carbon API Service")
    
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=10)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"Healthcheck успешен: {data}")
        
        if data.get("status") != "healthy":
            logger.warning(f"API сервис не полностью работоспособен: {data}")
        
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при проверке healthcheck: {e}")
        raise


def trigger_data_collection() -> dict:
    """
    Триггерить сбор данных через API сервис.
    
    Вызывает POST /collect эндпоинт для запуска сбора данных.
    
    Returns:
        dict: Результат сбора данных
    """
    logger.info("Запуск сбора данных через API сервис")
    
    try:
        response = requests.post(f"{API_BASE_URL}/collect", timeout=30)
        response.raise_for_status()
        
        data = response.json()
        logger.info(
            f"Сбор данных завершен: "
            f"собрано {data.get('records_collected')}, "
            f"сохранено {data.get('records_saved')}"
        )
        
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при сборе данных: {e}")
        raise


def get_statistics() -> dict:
    """
    Получить статистику по собранным данным.
    
    Returns:
        dict: Статистика
    """
    logger.info("Получение статистики через API сервис")
    
    try:
        response = requests.get(f"{API_BASE_URL}/statistics", timeout=10)
        response.raise_for_status()
        
        data = response.json()
        logger.info(
            f"Статистика: "
            f"всего записей {data.get('total_records')}, "
            f"последняя запись {data.get('latest_record_time')}"
        )
        
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при получении статистики: {e}")
        raise


def get_latest_data(limit: int = 5) -> list:
    """
    Получить последние записи.
    
    Args:
        limit: Количество записей
        
    Returns:
        list: Последние записи
    """
    logger.info(f"Получение последних {limit} записей через API сервис")
    
    try:
        response = requests.get(
            f"{API_BASE_URL}/data/latest",
            params={"limit": limit},
            timeout=10
        )
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"Получено {len(data)} записей")
        
        # Вывод примера первой записи
        if data:
            logger.info(f"Пример записи: {data[0]}")
        
        return data
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка при получении данных: {e}")
        raise


def validate_data_freshness() -> bool:
    """
    Проверить свежесть данных.
    
    Проверяет, что данные были собраны недавно (в пределах 2 часов).
    
    Returns:
        bool: True если данные свежие
    """
    logger.info("Проверка свежести данных")
    
    try:
        stats = get_statistics()
        latest_time_str = stats.get("latest_record_time")
        
        if not latest_time_str:
            logger.warning("Нет данных в системе")
            return False
        
        # Парсинг времени
        from datetime import datetime, timezone
        latest_time = datetime.fromisoformat(latest_time_str.replace('Z', '+00:00'))
        current_time = datetime.now(timezone.utc)
        
        time_diff = current_time - latest_time
        hours_old = time_diff.total_seconds() / 3600
        
        logger.info(f"Последние данные были собраны {hours_old:.2f} часов назад")
        
        if hours_old > 2:
            logger.warning(f"Данные устарели! Последний сбор был {hours_old:.2f} часов назад")
            return False
        
        logger.info("Данные свежие")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при проверке свежести данных: {e}")
        raise


# DAG определение
with DAG(
    dag_id="carbon_api_service_collection",
    default_args=default_args,
    description="Сбор данных о carbon intensity через HTTP API сервис",
    schedule_interval="*/30 * * * *",  # Каждые 30 минут
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["carbon-intensity", "api-service", "data-collection"],
) as dag:
    
    # Проверка работоспособности API сервиса
    health_check_task = PythonOperator(
        task_id="check_api_health",
        python_callable=check_api_health,
    )
    
    # Триггер сбора данных
    collect_data_task = PythonOperator(
        task_id="trigger_data_collection",
        python_callable=trigger_data_collection,
    )
    
    # Получение статистики
    statistics_task = PythonOperator(
        task_id="get_statistics",
        python_callable=get_statistics,
    )
    
    # Получение последних записей
    latest_data_task = PythonOperator(
        task_id="get_latest_data",
        python_callable=get_latest_data,
        op_kwargs={"limit": 5},
    )
    
    # Проверка свежести данных
    validate_freshness_task = PythonOperator(
        task_id="validate_data_freshness",
        python_callable=validate_data_freshness,
    )
    
    # Порядок выполнения задач
    health_check_task >> collect_data_task >> [statistics_task, latest_data_task] >> validate_freshness_task

