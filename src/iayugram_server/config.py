"""Environment-backed configuration."""
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    api_id: int
    api_hash: str
    session_string: str = ""
    content_key: str  # Fernet key (base64)
    # "symmetric" (Fernet, server can read) is current. Reserved for a future
    # "sealed" mode: encrypt at rest with the client's public key so the server
    # operator can't read the archive — deferred until the iOS client exists to
    # hold the private key. Per-account keys already isolate accounts either way.
    content_key_type: str = "symmetric"

    host: str = "0.0.0.0"
    port: int = 8787
    client_token: str = "change-me"

    db_path: str = "data/iayugram.db"
    content_retention_hours: int = 168

    # Media capture (strategy A: download on receipt, filtered by type + size).
    # Captures photos, stickers, voice notes and round video, and — since phase 2 —
    # video, animations, music and documents. View-once media is downloaded
    # silently (download never sends a read/consumed update).
    media_capture: bool = True
    # Phase 2: media streams to disk and is encrypted chunk-by-chunk, so the limit
    # is about disk and the client's willingness to download, not server RAM.
    media_max_bytes: int = 512 * 1024 * 1024
    # Plaintext bytes per encrypted frame; also the read granularity when serving a
    # byte range. 1 MiB keeps per-request memory flat.
    media_chunk_bytes: int = 1024 * 1024
    media_dir: str = "data/media"            # encrypted media files live here
    media_retention_hours: int = 720         # 30 days
    # Upper bound on a single download. A cross-DC transfer can stall indefinitely;
    # without this the capture just loses the media and logs nothing.
    media_download_timeout: int = 300

    # On startup, verify stored messages still exist (getMessages by ID) and emit
    # synthetic DELETED events for any that vanished while the server was down.
    # StringSession does not persist Telethon's update pts across restarts, so
    # this scan is the ONLY way to recover deletes missed during downtime.
    reconcile_on_launch: bool = True
    reconcile_max_messages: int = 3000  # cap per launch to bound API load


settings = Settings()  # type: ignore[call-arg]
