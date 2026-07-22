from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="DISPATCH_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    environment: str = "production"
    database_url: SecretStr
    organization_id: str = "default"
    organization_name: str = "Dispatch Core"
    default_pack: str = "field_service"
    admin_api_key: SecretStr
    auto_migrate: bool = True
    migrations_directory: Path | None = None
    webhook_max_body_bytes: int = Field(default=1_048_576, ge=1024, le=10_485_760)
    telegram_webhook_secret: SecretStr | None = None
    max_webhook_secret: SecretStr | None = None
    telegram_bot_token: SecretStr | None = None
    telegram_bot_token_file: Path | None = None
    telegram_receive_mode: Literal["disabled", "polling", "webhook"] = "disabled"
    telegram_proxy: SecretStr | None = None
    max_bot_token: SecretStr | None = None
    max_bot_token_file: Path | None = None
    max_receive_mode: Literal["disabled", "polling", "webhook"] = "disabled"
    max_proxy: SecretStr | None = None
    callback_signing_secret: SecretStr | None = None
    worker_idle_seconds: float = Field(default=0.25, ge=0.05, le=10)
    api_host: str = "0.0.0.0"
    api_port: int = Field(default=8080, ge=1, le=65535)

    @field_validator("admin_api_key")
    @classmethod
    def validate_api_key(cls, value: SecretStr) -> SecretStr:
        if len(value.get_secret_value()) < 32:
            raise ValueError("admin API key must contain at least 32 characters")
        return value

    @field_validator("organization_id", "organization_name", "api_host")
    @classmethod
    def validate_non_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("organization fields cannot be blank")
        return value.strip()

    @model_validator(mode="after")
    def validate_webhook_secrets(self) -> Settings:
        pairs = (
            (self.telegram_receive_mode, self.telegram_webhook_secret, "Telegram"),
            (self.max_receive_mode, self.max_webhook_secret, "MAX"),
        )
        for mode, secret, provider in pairs:
            if mode == "webhook" and (
                secret is None or not secret.get_secret_value().strip()
            ):
                raise ValueError(f"{provider} webhook mode requires a secret")
        return self
