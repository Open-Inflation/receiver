from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from app.config import Settings
from app.dashboard_app import create_dashboard_app


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        database_url=f"sqlite:///{tmp_path / 'dashboard-test.sqlite3'}",
        storage_base_url="http://127.0.0.1:9999",
        storage_api_token="test-token",
        parser_src_path=(Path(__file__).resolve().parents[2] / "parser" / "src"),
        lease_ttl_minutes=30,
    )


def test_dashboard_crud_and_overview(tmp_path: Path):
    app = create_dashboard_app(_settings(tmp_path))
    with TestClient(app) as client:
        page = client.get("/")
        assert page.status_code == 200
        assert "Receiver Control Room" in page.text

        create = client.post(
            "/api/tasks",
            json={
                "city": "Moscow",
                "store": "C500",
                "frequency_hours": 24,
                "parser_name": "fixprice",
                "is_active": True,
            },
        )
        assert create.status_code == 201
        task_id = create.json()["id"]

        update = client.patch(
            f"/api/tasks/{task_id}",
            json={
                "frequency_hours": 12,
                "is_active": False,
            },
        )
        assert update.status_code == 200
        assert update.json()["frequency_hours"] == 12
        assert update.json()["is_active"] is False

        tasks = client.get("/api/tasks")
        assert tasks.status_code == 200
        assert len(tasks.json()) == 1

        overview = client.get("/api/overview")
        assert overview.status_code == 200
        body = overview.json()
        assert body["tasks_total"] == 1
        assert body["tasks_active"] == 0
        assert body["runs_total"] == 0

        forbidden_update = client.patch(
            f"/api/tasks/{task_id}",
            json={
                "last_crawl_at": None,
            },
        )
        assert forbidden_update.status_code == 422
