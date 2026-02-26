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
    image_upload_parallelism: int = 4
    image_archive_max_file_bytes: int = 12 * 1024 * 1024
    image_archive_max_files: int = 2000
    artifact_download_max_bytes: int = 256 * 1024 * 1024
    artifact_json_member_max_bytes: int = 16 * 1024 * 1024
    orchestrator_ws_url: str = "ws://127.0.0.1:8765"
    orchestrator_ws_password: str | None = None
    orchestrator_poll_interval_sec: float = 5.0
    orchestrator_ws_request_timeout_sec: float = 15.0
    orchestrator_max_claims_per_cycle: int = 5
    orchestrator_assigned_parallelism: int = 4
    orchestrator_manager_name: str = "parser-ws"
    orchestrator_auto_dispatch_enabled: bool = True
    orchestrator_submit_include_images: bool = True
    orchestrator_submit_full_catalog: bool = True
    orchestrator_upload_archive_images: bool = True


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    token = raw.strip().lower()
    if token in {"1", "true", "yes", "y", "on"}:
        return True
    if token in {"0", "false", "no", "n", "off"}:
        return False
    return default


def load_settings() -> Settings:
    database_url = os.getenv("DATABASE_URL", f"sqlite:///{DEFAULT_SQLITE_PATH}")
    storage_base_url = os.getenv("STORAGE_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
    storage_api_token = os.getenv("STORAGE_API_TOKEN", "change-me-token")
    parser_src_path = Path(os.getenv("PARSER_SRC_PATH", "../parser/src")).expanduser().resolve()
    orchestrator_ws_url = os.getenv("ORCHESTRATOR_WS_URL", "ws://127.0.0.1:8765").strip()
    orchestrator_ws_password = os.getenv("ORCHESTRATOR_WS_PASSWORD")
    orchestrator_manager_name = os.getenv("ORCHESTRATOR_MANAGER_NAME", "parser-ws").strip() or "parser-ws"

    lease_ttl_raw = os.getenv("LEASE_TTL_MINUTES", "30")
    try:
        lease_ttl_minutes = max(1, int(lease_ttl_raw))
    except ValueError:
        lease_ttl_minutes = 30

    poll_raw = os.getenv("ORCHESTRATOR_POLL_INTERVAL_SEC", "5")
    try:
        poll_interval = max(0.5, float(poll_raw))
    except ValueError:
        poll_interval = 5.0

    ws_request_timeout_raw = os.getenv("ORCHESTRATOR_WS_REQUEST_TIMEOUT_SEC", "15")
    try:
        ws_request_timeout_sec = max(1.0, float(ws_request_timeout_raw))
    except ValueError:
        ws_request_timeout_sec = 15.0

    image_parallel_raw = os.getenv("IMAGE_UPLOAD_PARALLELISM", "4")
    try:
        image_upload_parallelism = max(1, int(image_parallel_raw))
    except ValueError:
        image_upload_parallelism = 4

    image_archive_max_file_raw = os.getenv("IMAGE_ARCHIVE_MAX_FILE_BYTES", str(12 * 1024 * 1024))
    try:
        image_archive_max_file_bytes = max(1, int(image_archive_max_file_raw))
    except ValueError:
        image_archive_max_file_bytes = 12 * 1024 * 1024

    image_archive_max_files_raw = os.getenv("IMAGE_ARCHIVE_MAX_FILES", "2000")
    try:
        image_archive_max_files = max(1, int(image_archive_max_files_raw))
    except ValueError:
        image_archive_max_files = 2000

    artifact_download_max_raw = os.getenv("ARTIFACT_DOWNLOAD_MAX_BYTES", str(256 * 1024 * 1024))
    try:
        artifact_download_max_bytes = max(1, int(artifact_download_max_raw))
    except ValueError:
        artifact_download_max_bytes = 256 * 1024 * 1024

    artifact_json_member_max_raw = os.getenv("ARTIFACT_JSON_MEMBER_MAX_BYTES", str(16 * 1024 * 1024))
    try:
        artifact_json_member_max_bytes = max(1, int(artifact_json_member_max_raw))
    except ValueError:
        artifact_json_member_max_bytes = 16 * 1024 * 1024

    max_claims_raw = os.getenv("ORCHESTRATOR_MAX_CLAIMS_PER_CYCLE", "5")
    try:
        orchestrator_max_claims_per_cycle = max(1, int(max_claims_raw))
    except ValueError:
        orchestrator_max_claims_per_cycle = 5

    assigned_parallel_raw = os.getenv("ORCHESTRATOR_ASSIGNED_PARALLELISM", "4")
    try:
        orchestrator_assigned_parallelism = max(1, int(assigned_parallel_raw))
    except ValueError:
        orchestrator_assigned_parallelism = 4

    return Settings(
        database_url=database_url,
        storage_base_url=storage_base_url,
        storage_api_token=storage_api_token,
        parser_src_path=parser_src_path,
        lease_ttl_minutes=lease_ttl_minutes,
        image_upload_parallelism=image_upload_parallelism,
        image_archive_max_file_bytes=image_archive_max_file_bytes,
        image_archive_max_files=image_archive_max_files,
        artifact_download_max_bytes=artifact_download_max_bytes,
        artifact_json_member_max_bytes=artifact_json_member_max_bytes,
        orchestrator_ws_url=orchestrator_ws_url,
        orchestrator_ws_password=orchestrator_ws_password if orchestrator_ws_password else None,
        orchestrator_poll_interval_sec=poll_interval,
        orchestrator_ws_request_timeout_sec=ws_request_timeout_sec,
        orchestrator_max_claims_per_cycle=orchestrator_max_claims_per_cycle,
        orchestrator_assigned_parallelism=orchestrator_assigned_parallelism,
        orchestrator_manager_name=orchestrator_manager_name,
        orchestrator_auto_dispatch_enabled=_env_bool("ORCHESTRATOR_AUTO_DISPATCH_ENABLED", True),
        orchestrator_submit_include_images=_env_bool("ORCHESTRATOR_SUBMIT_INCLUDE_IMAGES", True),
        orchestrator_submit_full_catalog=_env_bool("ORCHESTRATOR_SUBMIT_FULL_CATALOG", True),
        orchestrator_upload_archive_images=_env_bool("ORCHESTRATOR_UPLOAD_ARCHIVE_IMAGES", True),
    )
