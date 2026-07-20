"""Environment-backed configuration."""
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    api_id: int
    api_hash: str
    session_string: str = ""
    content_key: str  # Fernet key (base64)

    host: str = "0.0.0.0"
    port: int = 8787
    client_token: str = "change-me"

    db_path: str = "data/iayugram.db"
    content_retention_hours: int = 168


settings = Settings()  # type: ignore[call-arg]
