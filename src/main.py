"""Модуль приложения для сбора данных о carbon intensity."""

import logging
import signal
import sys
import time
from typing import Optional

import schedule
from carbon_api import CarbonIntensityAPI
from config import config
from database import MongoDBClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logger = logging.getLogger(__name__)


class CarbonIntensityCollector:
    """Класс для сбора и хранения данных о carbon intensity."""

    def __init__(self):
        """Инициализация коллектора данных."""
        self.db_client: Optional[MongoDBClient] = None
        self.api_client: Optional[CarbonIntensityAPI] = None
        self.running = True

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Обработчик сигналов для корректного завершения."""
        logger.info(f"Получен сигнал {signum}, завершение работы...")
        self.running = False

    def initialize(self) -> bool:
        """
        Инициализировать клиенты для работы с БД и API.
        """
        try:
            logger.info("Инициализация приложения...")

            self.db_client = MongoDBClient()

            self.api_client = CarbonIntensityAPI()

            logger.info("Приложение успешно инициализировано")
            return True
        except Exception as e:
            logger.error(f"Ошибка при инициализации: {e}")
            return False

    def collect_and_store_data(self) -> None:
        """Собрать данные из API и сохранить в MongoDB."""
        try:
            logger.info("=" * 50)
            logger.info("Начало сбора данных")

            # Получение данных за сегодня (содержит и прогноз, и факт)
            forecast_data = self.api_client.get_intensity_today()

            if not forecast_data:
                logger.warning("Не удалось получить данные из API")
                return

            logger.info(f"Получено {len(forecast_data)} записей из API")

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
            if records_to_insert:
                inserted_count = self.db_client.insert_many_carbon_intensity_data(records_to_insert)
                logger.info(f"Успешно сохранено {inserted_count} записей в MongoDB")

                # Статистика
                total_records = self.db_client.get_records_count()
                logger.info(f"Всего записей в БД: {total_records}")
            else:
                logger.warning("Нет данных для вставки")

            logger.info("Сбор данных завершен")

        except Exception as e:
            logger.error(f"Ошибка при сборе и сохранении данных: {e}", exc_info=True)

    def run_scheduled(self) -> None:
        """Запустить сбор данных по расписанию."""
        logger.info("Запуск планировщика задач")
        logger.info(f"Интервал сбора данных: {config.COLLECTION_INTERVAL} секунд")

        # Первый сбор данных сразу при запуске
        self.collect_and_store_data()

        # Настройка расписания
        schedule.every(config.COLLECTION_INTERVAL).seconds.do(self.collect_and_store_data)

        while self.running:
            try:
                schedule.run_pending()
                time.sleep(1)
            except Exception as e:
                logger.error(f"Ошибка в главном цикле: {e}")
                time.sleep(10)

        logger.info("Планировщик остановлен")

    def run_once(self) -> None:
        """Выполнить сбор данных один раз (для тестирования)."""
        logger.info("Режим одноразового выполнения")
        self.collect_and_store_data()

    def shutdown(self) -> None:
        """Корректное завершение работы приложения."""
        logger.info("Завершение работы приложения...")

        if self.db_client:
            self.db_client.close()

        logger.info("Приложение остановлено")


def main():
    """Главная функция приложения."""
    logger.info("Запуск Carbon Intensity Data Collector")
    logger.info(f"Конфигурация: {config}")

    collector = CarbonIntensityCollector()

    if not collector.initialize():
        logger.error("Не удалось инициализировать приложение")
        sys.exit(1)

    try:
        # Запуск в режиме планировщика
        collector.run_scheduled()
    except KeyboardInterrupt:
        logger.info("Получен сигнал прерывания")
    finally:
        collector.shutdown()

    logger.info("Приложение завершено")


if __name__ == "__main__":
    main()
