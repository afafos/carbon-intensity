"""Модуль для работы с MongoDB."""

import logging
from datetime import datetime
from typing import Any, Optional

from config import config
from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import PyMongoError

logger = logging.getLogger(__name__)


class MongoDBClient:
    """Клиент для работы с MongoDB."""

    def __init__(self):
        """Инициализация клиента MongoDB."""
        self.client: Optional[MongoClient] = None
        self.db: Optional[Database] = None
        self._connect()

    def _connect(self) -> None:
        """Установить соединение с MongoDB."""
        try:
            self.client = MongoClient(config.mongodb_uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command("ping")
            self.db = self.client[config.MONGODB_DATABASE]
            logger.info(f"Успешное подключение к MongoDB: {config.MONGODB_DATABASE}")

            self._create_indexes()
        except PyMongoError as e:
            logger.error(f"Ошибка подключения к MongoDB: {e}")
            raise

    def _create_indexes(self) -> None:
        """Создать индексы для коллекции carbon_intensity_data."""
        try:
            intensity_collection = self.db[config.COLLECTION_INTENSITY]
            intensity_collection.create_index([("from", ASCENDING)])
            intensity_collection.create_index([("to", ASCENDING)])
            intensity_collection.create_index([("created_at", ASCENDING)])

            logger.info("Индексы успешно созданы")
        except PyMongoError as e:
            logger.warning(f"Ошибка при создании индексов: {e}")

    def get_collection(self, collection_name: str) -> Collection:
        """
        Получить коллекцию по имени.
        """
        return self.db[collection_name]

    def insert_carbon_intensity_data(self, data: dict[str, Any]) -> Optional[str]:
        """
        Вставить данные о carbon intensity в MongoDB.
        """
        try:
            collection = self.get_collection(config.COLLECTION_INTENSITY)
            data["created_at"] = datetime.utcnow()
            result = collection.insert_one(data)
            logger.info(f"Данные успешно вставлены: {result.inserted_id}")
            return str(result.inserted_id)
        except PyMongoError as e:
            logger.error(f"Ошибка при вставке данных: {e}")
            return None

    def insert_many_carbon_intensity_data(self, data_list: list[dict[str, Any]]) -> int:
        """
        Вставить несколько записей о carbon intensity с защитой от дубликатов.
        Использует upsert по ключу (from, to).
        """
        if not data_list:
            return 0

        try:
            collection = self.get_collection(config.COLLECTION_INTENSITY)
            created_at = datetime.utcnow()

            # Используем bulk_write для эффективной вставки с upsert
            from pymongo import UpdateOne

            operations = []
            for data in data_list:
                data["created_at"] = created_at

                # Уникальный ключ - временной интервал (from, to)
                filter_query = {"from": data["from"], "to": data["to"]}

                # Обновить или вставить
                operations.append(UpdateOne(filter_query, {"$set": data}, upsert=True))

            result = collection.bulk_write(operations)
            count = result.upserted_count + result.modified_count
            logger.info(
                f"Обработано: {len(data_list)} записей "
                f"(вставлено: {result.upserted_count}, обновлено: {result.modified_count})"
            )
            return count
        except PyMongoError as e:
            logger.error(f"Ошибка при массовой вставке данных: {e}")
            return 0

    def get_latest_records(self, limit: int = 10) -> list[dict[str, Any]]:
        """
        Получить последние записи.
        """
        try:
            collection = self.get_collection(config.COLLECTION_INTENSITY)
            cursor = collection.find().sort("created_at", -1).limit(limit)
            return list(cursor)
        except PyMongoError as e:
            logger.error(f"Ошибка при получении записей: {e}")
            return []

    def get_records_count(self) -> int:
        """
        Получить количество записей в коллекции.
        """
        try:
            collection = self.get_collection(config.COLLECTION_INTENSITY)
            return collection.count_documents({})
        except PyMongoError as e:
            logger.error(f"Ошибка при подсчете записей: {e}")
            return 0

    def get_max_created_at(self) -> Optional[datetime]:
        """
        Получить максимальную дату created_at из коллекции.
        """
        try:
            collection = self.get_collection(config.COLLECTION_INTENSITY)
            result = collection.find_one({}, {"created_at": 1}, sort=[("created_at", -1)])
            if result and "created_at" in result:
                logger.info(f"Максимальный created_at в MongoDB: {result['created_at']}")
                return result["created_at"]
            logger.info("MongoDB пустая или нет created_at")
            return None
        except PyMongoError as e:
            logger.error(f"Ошибка при получении max created_at: {e}")
            return None

    def close(self) -> None:
        """Закрыть соединение с MongoDB."""
        if self.client:
            self.client.close()
            logger.info("Соединение с MongoDB закрыто")
