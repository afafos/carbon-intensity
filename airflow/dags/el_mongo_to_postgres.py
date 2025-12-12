"""
DAG для переноса данных из MongoDB в PostgreSQL (EL процесс).

Описание:
    Extract-Load процесс, который переносит данные о carbon intensity
    из MongoDB (source) в PostgreSQL (analytical DWH) с добавлением
    версионности по стратегии SCD Type 2.

Расписание:
    Каждый час (0 */1 * * *)

Шаги:
    1. extract_from_mongodb - извлечение новых данных из MongoDB
    2. transform_and_load_to_postgres - трансформация и загрузка в PostgreSQL
    3. log_statistics - логирование статистики загрузки

Особенности:
    - Инкрементальная загрузка (только новые данные)
    - SCD Type 2: полная история изменений прогнозов
    - Версионность: каждое обновление прогноза = новая версия
    - Upsert логика для текущих версий (is_current_version=true)

Целевая таблица:
    raw.carbon_intensity в PostgreSQL (analytics база)
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

# Параметры по умолчанию для DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def extract_from_mongodb(**context) -> int:
    """
    Извлечь данные из MongoDB инкрементально.
    При первом запуске берет все данные.
    При последующих - только новые (created_at > max(src_processed_dttm) в PostgreSQL).

    Returns:
        int: Количество извлеченных записей
    """
    logger.info("Начало извлечения данных из MongoDB")

    try:
        mongo_hook = MongoHook(conn_id="mongo_default")
        collection = mongo_hook.get_collection(
            mongo_collection="carbon_intensity_data", mongo_db="carbon_intensity"
        )

        postgres_hook = PostgresHook(postgres_conn_id="postgres_analytics")
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT MAX(src_processed_dttm)
            FROM raw.carbon_intensity
        """
        )
        result = cursor.fetchone()
        max_src_processed = result[0] if result and result[0] else None

        cursor.close()
        conn.close()

        if max_src_processed:
            # Инкрементальная загрузка: только новые данные
            query = {"created_at": {"$gt": max_src_processed}}
            logger.info(f"Инкрементальная загрузка: created_at > {max_src_processed}")
        else:
            # Первая загрузка: все данные
            query = {}
            logger.info("Первая загрузка: забираем все данные")

        cursor = collection.find(query)
        documents = list(cursor)

        logger.info(f"Извлечено {len(documents)} записей из MongoDB")

        # Конвертируем ObjectId в строку для JSON сериализации
        for doc in documents:
            if "_id" in doc:
                doc["_id"] = str(doc["_id"])

        # Сохраняем данные в XCom для следующей задачи
        ti = context["ti"]
        ti.xcom_push(key="extracted_data", value=documents)

        return len(documents)

    except Exception as e:
        logger.error(f"Ошибка при извлечении данных из MongoDB: {e}")
        raise


def transform_data(**context) -> int:
    """
    Трансформировать данные перед загрузкой в PostgreSQL.

    Returns:
        int: Количество трансформированных записей
    """
    logger.info("Начало трансформации данных")

    ti = context["ti"]
    documents = ti.xcom_pull(key="extracted_data", task_ids="extract_from_mongodb")

    if not documents:
        logger.info("Нет данных для трансформации")
        return 0

    transformed_records = []

    for doc in documents:
        try:
            # Конвертируем MongoDB документ в формат для PostgreSQL
            record = {
                "data_from_dttm": doc.get("from"),
                "data_to_dttm": doc.get("to"),
                "forecast_intensity": doc.get("intensity", {}).get("forecast"),
                "actual_intensity": doc.get("intensity", {}).get("actual"),
                "index_forecast": doc.get("intensity", {}).get("index"),
                "index_actual": doc.get("intensity", {}).get("index"),
                "src_processed_dttm": doc.get("created_at"),
            }

            transformed_records.append(record)

        except Exception as e:
            logger.warning(f"Ошибка при трансформации записи: {e}")
            continue

    logger.info(f"Трансформировано {len(transformed_records)} записей")

    # Сохраняем трансформированные данные
    ti.xcom_push(key="transformed_data", value=transformed_records)

    return len(transformed_records)


def load_to_postgres(**context) -> int:
    """
    Загрузить данные в PostgreSQL с версионностью (SCD Type 2).
    Если данные изменились - закрывает старую версию и создает новую.

    Returns:
        int: Количество обработанных записей
    """
    logger.info("Начало загрузки данных в PostgreSQL (SCD Type 2)")

    ti = context["ti"]
    records = ti.xcom_pull(key="transformed_data", task_ids="transform_data")

    if not records:
        logger.info("Нет данных для загрузки")
        return 0

    try:
        # Подключение к PostgreSQL
        postgres_hook = PostgresHook(postgres_conn_id="postgres_analytics")
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        inserted_count = 0
        updated_count = 0

        for record in records:
            try:
                data_from = record["data_from_dttm"]
                data_to = record["data_to_dttm"]

                # Проверяем есть ли активная запись для этого интервала
                cursor.execute(
                    """
                    SELECT
                        id,
                        forecast_intensity,
                        actual_intensity,
                        index_forecast,
                        index_actual
                    FROM raw.carbon_intensity
                    WHERE data_from_dttm = %(data_from)s
                      AND data_to_dttm = %(data_to)s
                      AND valid_to_dttm = '5999-01-01 00:00:00'
                """,
                    {"data_from": data_from, "data_to": data_to},
                )

                existing = cursor.fetchone()

                # Проверяем изменились ли данные
                if existing:
                    old_id, old_forecast, old_actual, old_idx_f, old_idx_a = existing

                    # Сравниваем данные
                    data_changed = (
                        record["forecast_intensity"] != old_forecast
                        or record["actual_intensity"] != old_actual
                        or record["index_forecast"] != old_idx_f
                        or record["index_actual"] != old_idx_a
                    )

                    if data_changed:
                        # Получить текущее время без миллисекунд
                        cursor.execute("SELECT DATE_TRUNC('second', CURRENT_TIMESTAMP)")
                        new_valid_from = cursor.fetchone()[0]

                        # Закрываем старую версию (valid_to = новое время - 1 секунда)
                        cursor.execute(
                            """
                            UPDATE raw.carbon_intensity
                            SET valid_to_dttm = %(new_valid_from)s - INTERVAL '1 second'
                            WHERE id = %(id)s
                        """,
                            {"id": old_id, "new_valid_from": new_valid_from},
                        )

                        # Вставляем новую версию с округленным временем
                        cursor.execute(
                            """
                            INSERT INTO raw.carbon_intensity (
                                data_from_dttm,
                                data_to_dttm,
                                forecast_intensity,
                                actual_intensity,
                                index_forecast,
                                index_actual,
                                valid_from_dttm,
                                valid_to_dttm,
                                src_processed_dttm
                            ) VALUES (
                                %(data_from_dttm)s,
                                %(data_to_dttm)s,
                                %(forecast_intensity)s,
                                %(actual_intensity)s,
                                %(index_forecast)s,
                                %(index_actual)s,
                                %(new_valid_from)s,
                                '5999-01-01 00:00:00',
                                %(src_processed_dttm)s
                            )
                        """,
                            {**record, "new_valid_from": new_valid_from},
                        )

                        updated_count += 1
                        logger.debug(f"Создана новая версия для {data_from} - {data_to}")
                    else:
                        logger.debug(f"Данные не изменились для {data_from} - {data_to}")
                else:
                    # Получить текущее время без миллисекунд
                    cursor.execute("SELECT DATE_TRUNC('second', CURRENT_TIMESTAMP)")
                    new_valid_from = cursor.fetchone()[0]

                    # Вставляем первую версию с округленным временем
                    cursor.execute(
                        """
                        INSERT INTO raw.carbon_intensity (
                            data_from_dttm,
                            data_to_dttm,
                            forecast_intensity,
                            actual_intensity,
                            index_forecast,
                            index_actual,
                            valid_from_dttm,
                            valid_to_dttm,
                            src_processed_dttm
                        ) VALUES (
                            %(data_from_dttm)s,
                            %(data_to_dttm)s,
                            %(forecast_intensity)s,
                            %(actual_intensity)s,
                            %(index_forecast)s,
                            %(index_actual)s,
                            %(new_valid_from)s,
                            '5999-01-01 00:00:00',
                            %(src_processed_dttm)s
                        )
                    """,
                        {**record, "new_valid_from": new_valid_from},
                    )

                    inserted_count += 1
                    logger.debug(f"Вставлена новая запись для {data_from} - {data_to}")

            except Exception as e:
                logger.warning(f"Ошибка при обработке записи: {e}")
                conn.rollback()
                continue

        conn.commit()
        cursor.close()
        conn.close()

        logger.info(
            f"Загрузка завершена: "
            f"новых записей: {inserted_count}, "
            f"обновлено (новых версий): {updated_count}"
        )

        return inserted_count + updated_count

    except Exception as e:
        logger.error(f"Ошибка при загрузке данных в PostgreSQL: {e}")
        raise


def validate_data_transfer(**context) -> bool:
    """
    Проверить успешность переноса данных.

    Returns:
        bool: True если валидация прошла успешно
    """
    logger.info("Валидация переноса данных")

    ti = context["ti"]

    # Получаем количество извлеченных и загруженных записей
    extracted_count = ti.xcom_pull(key="return_value", task_ids="extract_from_mongodb")
    loaded_count = ti.xcom_pull(key="return_value", task_ids="load_to_postgres")

    logger.info(f"Извлечено: {extracted_count}, Загружено: {loaded_count}")

    if extracted_count == 0:
        logger.info("Нет новых данных для переноса")
        return True

    if loaded_count >= extracted_count * 0.9:  # 90% успешности
        logger.info("Валидация пройдена успешно")
        return True
    else:
        logger.warning(
            f"Загружено меньше данных чем ожидалось: " f"{loaded_count}/{extracted_count}"
        )
        return False


# Определение DAG
with DAG(
    dag_id="el_mongo_to_postgres",
    default_args=default_args,
    description="EL процесс: перенос данных из MongoDB в PostgreSQL",
    schedule_interval="@hourly",  # Каждый час
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["el", "mongodb", "postgres", "carbon-intensity"],
) as dag:
    # Задача 1: Извлечение данных из MongoDB
    extract_task = PythonOperator(
        task_id="extract_from_mongodb",
        python_callable=extract_from_mongodb,
        provide_context=True,
    )

    # Задача 2: Трансформация данных
    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        provide_context=True,
    )

    # Задача 3: Загрузка данных в PostgreSQL
    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
        provide_context=True,
    )

    # Задача 4: Валидация
    validate_task = PythonOperator(
        task_id="validate_data_transfer",
        python_callable=validate_data_transfer,
        provide_context=True,
    )

    # Определение порядка выполнения задач
    extract_task >> transform_task >> load_task >> validate_task
