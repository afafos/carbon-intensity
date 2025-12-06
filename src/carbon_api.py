"""Модуль для работы с Carbon Intensity API."""

import logging
from typing import Any, Optional

import requests
from config import config

logger = logging.getLogger(__name__)


class CarbonIntensityAPI:
    """Клиент для работы с Carbon Intensity API."""

    def __init__(self):
        """Инициализация API клиента."""
        self.base_url = config.CARBON_API_BASE_URL
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def _make_request(self, endpoint: str) -> Optional[dict[str, Any]]:
        """
        Выполнить HTTP запрос к API.
        """
        url = f"{self.base_url}{endpoint}"
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка при запросе к API {url}: {e}")
            return None

    def get_intensity_today(self) -> Optional[list[dict[str, Any]]]:
        """
        Получить данные о carbon intensity за текущие сутки (24 часа).

        API endpoint: /intensity/date

        Возвращает:
            - 48 записей (интервалов по 30 минут)
            - Покрывают 24 часа (с 00:00 до 23:59)
            - Каждая запись - прогнозная и фактическая интенсивность CO2
        """
        logger.info("Получение данных за текущие сутки (48 интервалов × 30 мин = 24 часа)")
        response = self._make_request("/intensity/date")
        if response and "data" in response:
            return response["data"]
        return None
