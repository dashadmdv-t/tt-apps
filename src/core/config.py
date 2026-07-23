from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    api_key: str = Field(default="123", alias="API_KEY")
    db_url: str = Field(
        default="postgresql+asyncpg://postgres:postgres@localhost:5432/payments_service_db",
        alias="DB_URL",
    )
    db_url_sync: str = Field(
        default="postgresql://postgres:postgres@localhost:5432/payments_service_db",
        alias="DB_URL_SYNC",
    )
    db_echo: bool = Field(default=False, alias="DB_ECHO")
    rabbit_url: str = Field(
        default="amqp://guest:guest@rabbit_mq:5672/",
        alias="RABBIT_URL",
    )

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    return AppSettings()
