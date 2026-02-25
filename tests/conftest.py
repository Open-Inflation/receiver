from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


@pytest.fixture()
def client(tmp_path: Path):
    db_path = tmp_path / "receiver-test.sqlite3"

    settings = Settings(
        database_url=f"sqlite:///{db_path}",
        storage_base_url="http://127.0.0.1:9999",
        storage_api_token="test-token",
        parser_src_path=(Path(__file__).resolve().parents[2] / "parser" / "src"),
        lease_ttl_minutes=30,
        orchestrator_auto_dispatch_enabled=False,
    )

    app = create_app(settings)
    with TestClient(app) as test_client:
        yield test_client
