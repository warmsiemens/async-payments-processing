from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    api_key: SecretStr = Field(min_length=16)
    database_url: str = "postgresql+asyncpg://payments:payments@localhost:15432/payments"
    rabbitmq_url: str = "amqp://payments:payments@localhost:5672/"
    outbox_poll_interval: float = Field(default=0.5, gt=0)
    publish_timeout: float = Field(default=10, gt=0)
    retry_base_delay: float = Field(default=2, gt=0)
    webhook_timeout: float = Field(default=10, gt=0)
    worker_lease_seconds: float = Field(default=60, gt=10)
    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
