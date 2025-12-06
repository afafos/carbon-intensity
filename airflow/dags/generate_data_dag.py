"""
DAG для генерации нагрузки на Python сервис.
Периодически триггерит сбор данных из Carbon Intensity API.
"""

import logging
from datetime import datetime, timedelta

import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def check_data_collector_health() -> bool:
    """
    Проверить работоспособность сервиса сбора данных.

    Returns:
        bool: True если сервис работает
    """
    logger.info("Проверка работоспособности data-collector сервиса")

    try:
        mongo_hook = MongoHook(conn_id="mongo_default")
        collection = mongo_hook.get_collection(
            mongo_collection="carbon_intensity_data", mongo_db="carbon_intensity"
        )

        count = collection.count_documents({})
        logger.info(f"Всего записей в MongoDB: {count}")

        latest = collection.find_one(sort=[("created_at", -1)])

        if latest:
            logger.info(f"Последняя запись создана: {latest.get('created_at')}")
            return True
        else:
            logger.warning("Нет записей в MongoDB")
            return False

    except Exception as e:
        logger.error(f"Ошибка при проверке сервиса: {e}")
        return False


def trigger_data_collection() -> dict:
    """
    Триггер для сбора данных.

    В реальности, data-collector работает постоянно по расписанию.
    Функция просто проверяет, что данные поступают.

    Returns:
        dict: Статистика по данным
    """
    logger.info("Проверка поступления данных")

    try:
        mongo_hook = MongoHook(conn_id="mongo_default")
        collection = mongo_hook.get_collection(
            mongo_collection="carbon_intensity_data", mongo_db="carbon_intensity"
        )

        one_hour_ago = datetime.utcnow() - timedelta(hours=1)

        recent_count = collection.count_documents({"created_at": {"$gte": one_hour_ago}})

        total_count = collection.count_documents({})

        stats = {
            "total_records": total_count,
            "recent_records_1h": recent_count,
            "timestamp": datetime.utcnow().isoformat(),
        }

        logger.info(f"Статистика данных: {stats}")

        return stats

    except Exception as e:
        logger.error(f"Ошибка при получении статистики: {e}")
        raise


def check_api_availability() -> bool:
    """
    Проверить доступность Carbon Intensity API.

    Returns:
        bool: True если API доступен
    """
    logger.info("Проверка доступности Carbon Intensity API")

    api_url = "https://api.carbonintensity.org.uk/intensity"

    try:
        response = requests.get(api_url, timeout=10)

        if response.status_code == 200:
            logger.info("API доступен")
            data = response.json()
            logger.info(f"Получен ответ: {data.get('data', [{}])[0].get('intensity', {})}")
            return True
        else:
            logger.warning(f"API вернул статус: {response.status_code}")
            return False

    except Exception as e:
        logger.error(f"Ошибка при проверке API: {e}")
        return False


with DAG(
    dag_id="generate_data_load",
    default_args=default_args,
    description="Генерация нагрузки и проверка сбора данных",
    schedule_interval="*/30 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["monitoring", "data-generation", "carbon-intensity"],
) as dag:
    # Проверка доступности API
    check_api_task = PythonOperator(
        task_id="check_api_availability",
        python_callable=check_api_availability,
    )

    # Проверка работоспособности сервиса
    check_health_task = PythonOperator(
        task_id="check_data_collector_health",
        python_callable=check_data_collector_health,
    )

    # Триггер сбора данных
    trigger_task = PythonOperator(
        task_id="trigger_data_collection",
        python_callable=trigger_data_collection,
    )

    # Порядок выполнения
    check_api_task >> check_health_task >> trigger_task
