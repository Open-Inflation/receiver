from __future__ import annotations

from pathlib import Path

from sqlalchemy import create_engine, inspect, text

from app.config import Settings
from app.main import create_app


def _create_legacy_schema(db_path: Path) -> None:
    engine = create_engine(f"sqlite:///{db_path}")
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                CREATE TABLE orchestrators (
                    id VARCHAR(32) PRIMARY KEY,
                    name VARCHAR(120) NOT NULL UNIQUE,
                    token VARCHAR(128) NOT NULL UNIQUE,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL,
                    last_heartbeat_at DATETIME NOT NULL
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE crawl_tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    city VARCHAR(120) NOT NULL,
                    store VARCHAR(120) NOT NULL,
                    frequency_hours INTEGER NOT NULL,
                    last_crawl_at DATETIME,
                    parser_name VARCHAR(64) NOT NULL,
                    is_active BOOLEAN NOT NULL,
                    lease_owner_id VARCHAR(32),
                    lease_until DATETIME,
                    created_at DATETIME NOT NULL,
                    updated_at DATETIME NOT NULL
                )
                """
            )
        )
        connection.execute(
            text(
                """
                CREATE TABLE task_runs (
                    id VARCHAR(32) PRIMARY KEY,
                    task_id INTEGER NOT NULL,
                    orchestrator_id VARCHAR(32) NOT NULL,
                    status VARCHAR(16) NOT NULL,
                    assigned_at DATETIME NOT NULL,
                    finished_at DATETIME,
                    payload_json JSON,
                    parser_payload_json JSON,
                    error_message TEXT
                )
                """
            )
        )
    engine.dispose()


def test_legacy_schema_is_patched_on_startup(tmp_path: Path) -> None:
    db_path = tmp_path / "legacy.sqlite3"
    _create_legacy_schema(db_path)

    settings = Settings(
        database_url=f"sqlite:///{db_path}",
        storage_base_url="http://127.0.0.1:9999",
        storage_api_token="test-token",
        parser_src_path=(Path(__file__).resolve().parents[2] / "parser" / "src"),
        lease_ttl_minutes=30,
        orchestrator_auto_dispatch_enabled=False,
    )

    app = create_app(settings)
    inspector = inspect(app.state.engine)
    columns = {column["name"] for column in inspector.get_columns("task_runs")}
    task_columns = {column["name"] for column in inspector.get_columns("crawl_tasks")}
    assert "deleted_at" in task_columns
    assert "dispatch_meta_json" not in columns
    assert "processed_images" not in columns

    app.state.engine.dispose()
