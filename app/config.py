from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


DEFAULT_SQLITE_PATH = Path("data/receiver.db")


@dataclass(frozen=True, slots=True)
class Settings:
    database_url: str
    storage_base_url: str
    storage_api_token: str
    parser_src_path: Path
    lease_ttl_minutes: int


def load_settings() -> Settings:
    database_url = os.getenv("DATABASE_URL", f"sqlite:///{DEFAULT_SQLITE_PATH}")
    storage_base_url = os.getenv("STORAGE_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
    storage_api_token = os.getenv("STORAGE_API_TOKEN", "change-me-token")
    parser_src_path = Path(os.getenv("PARSER_SRC_PATH", "../parser/src")).expanduser().resolve()

    lease_ttl_raw = os.getenv("LEASE_TTL_MINUTES", "30")
    try:
        lease_ttl_minutes = max(1, int(lease_ttl_raw))
    except ValueError:
        lease_ttl_minutes = 30

    return Settings(
        database_url=database_url,
        storage_base_url=storage_base_url,
        storage_api_token=storage_api_token,
        parser_src_path=parser_src_path,
        lease_ttl_minutes=lease_ttl_minutes,
    )
