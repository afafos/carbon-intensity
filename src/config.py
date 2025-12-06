"""Конфигурация приложения для сбора данных о carbon intensity."""

import os

from dotenv import load_dotenv

load_dotenv()


class Config:
    """Класс конфигурации приложения."""

    MONGODB_HOST = os.getenv("MONGODB_HOST", "localhost")
    MONGODB_PORT = int(os.getenv("MONGODB_PORT", "27017"))
    MONGODB_USER = os.getenv("MONGODB_USER", "admin")
    MONGODB_PASSWORD = os.getenv("MONGODB_PASSWORD", "admin123")
    MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "carbon_intensity")

    COLLECTION_INTENSITY = "carbon_intensity_data"

    CARBON_API_BASE_URL = os.getenv(
        "CARBON_INTENSITY_API_BASE_URL", "https://api.carbonintensity.org.uk"
    )

    COLLECTION_INTERVAL = int(os.getenv("COLLECTION_INTERVAL", "1800"))  # 30 минут

    @property
    def mongodb_uri(self) -> str:
        """Создать URI строку для подключения к MongoDB."""
        return (
            f"mongodb://{self.MONGODB_USER}:{self.MONGODB_PASSWORD}"
            f"@{self.MONGODB_HOST}:{self.MONGODB_PORT}/"
        )

    def __repr__(self) -> str:
        """Представление конфигурации."""
        return (
            f"Config(mongodb_host={self.MONGODB_HOST}, "
            f"mongodb_database={self.MONGODB_DATABASE})"
        )


config = Config()
