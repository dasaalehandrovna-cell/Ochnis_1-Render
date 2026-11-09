from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    bot_token: str = Field(..., alias="BOT_TOKEN")
    render_external_url: str = Field(..., alias="RENDER_EXTERNAL_URL")
    webhook_secret: str = Field(..., alias="WEBHOOK_SECRET")
    admin_ids_raw: str = Field(default="", alias="ADMIN_IDS")

    port: int = Field(default=10000, alias="PORT")
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    webhook_path: str = Field(default="/telegram/webhook", alias="WEBHOOK_PATH")

    @property
    def admin_ids(self) -> set[int]:
        result: set[int] = set()
        for value in self.admin_ids_raw.split(","):
            value = value.strip()
            if value:
                try:
                    result.add(int(value))
                except ValueError as exc:
                    raise ValueError(
                        f"ADMIN_IDS содержит нечисловое значение: {value}"
                    ) from exc
        return result

    @property
    def normalized_base_url(self) -> str:
        return self.render_external_url.rstrip("/")

    @property
    def normalized_webhook_path(self) -> str:
        return "/" + self.webhook_path.strip("/")


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
