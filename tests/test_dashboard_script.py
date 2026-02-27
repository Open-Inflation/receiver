from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from app.config import Settings
from app.dashboard_app import create_dashboard_app
from app.models import CrawlTask, Orchestrator, RunArtifact, TaskRun
from app.services.scheduler import utcnow


def _settings(tmp_path: Path, *, ws_password: str | None = None) -> Settings:
    return Settings(
        database_url=f"sqlite:///{tmp_path / 'dashboard-test.sqlite3'}",
        storage_base_url="http://127.0.0.1:9999",
        storage_api_token="test-token",
        parser_src_path=(Path(__file__).resolve().parents[2] / "parser" / "src"),
        lease_ttl_minutes=30,
        orchestrator_ws_url="ws://127.0.0.1:8765",
        orchestrator_ws_password=ws_password,
    )


def test_dashboard_crud_and_overview(tmp_path: Path):
    app = create_dashboard_app(_settings(tmp_path))
    with TestClient(app) as client:
        page = client.get("/")
        assert page.status_code == 200
        assert "Receiver Control Room" in page.text
        assert "Картинки" in page.text
        validation_page = client.get("/validation-errors")
        assert validation_page.status_code == 200
        assert "Validation Fix Queue" in validation_page.text

        create = client.post(
            "/api/tasks",
            json={
                "city": "Moscow",
                "store": "C500",
                "frequency_hours": 24,
                "parser_name": "fixprice",
                "include_images": False,
                "is_active": True,
            },
        )
        assert create.status_code == 201
        task_id = create.json()["id"]

        update = client.patch(
            f"/api/tasks/{task_id}",
            json={
                "frequency_hours": 12,
                "include_images": True,
                "is_active": False,
            },
        )
        assert update.status_code == 200
        assert update.json()["frequency_hours"] == 12
        assert update.json()["include_images"] is True
        assert update.json()["is_active"] is False

        tasks = client.get("/api/tasks")
        assert tasks.status_code == 200
        assert len(tasks.json()) == 1
        listed_task = tasks.json()[0]
        assert listed_task["include_images"] is True
        assert listed_task["is_due"] is False
        assert str(listed_task["created_at"]).endswith("+00:00")
        assert str(listed_task["updated_at"]).endswith("+00:00")

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

        deleted = client.delete(f"/api/tasks/{task_id}")
        assert deleted.status_code == 200
        assert deleted.json()["ok"] is True

        tasks_after_delete = client.get("/api/tasks")
        assert tasks_after_delete.status_code == 200
        assert tasks_after_delete.json() == []


def test_dashboard_validation_errors_lists_only_unresolved_latest_success(tmp_path: Path):
    app = create_dashboard_app(_settings(tmp_path))
    session = app.state.session_factory()
    try:
        now = utcnow()
        orchestrator = Orchestrator(
            id="o" * 32,
            name="parser-ws-validation",
            token="t" * 40,
            created_at=now,
            updated_at=now,
            last_heartbeat_at=now,
        )
        task_resolved = CrawlTask(
            city="Moscow",
            store="C901",
            frequency_hours=24,
            parser_name="fixprice",
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        task_unresolved = CrawlTask(
            city="SPB",
            store="C902",
            frequency_hours=24,
            parser_name="chizhik",
            is_active=False,
            created_at=now,
            updated_at=now,
        )
        session.add_all([orchestrator, task_resolved, task_unresolved])
        session.commit()
        session.refresh(task_resolved)
        session.refresh(task_unresolved)

        resolved_old_run = TaskRun(
            id="a" * 32,
            task_id=task_resolved.id,
            orchestrator_id=orchestrator.id,
            status="success",
            assigned_at=now.replace(minute=0, second=0, microsecond=0),
            finished_at=now.replace(minute=1, second=0, microsecond=0),
            dispatch_meta_json={"remote_status": "success"},
        )
        resolved_new_run = TaskRun(
            id="b" * 32,
            task_id=task_resolved.id,
            orchestrator_id=orchestrator.id,
            status="success",
            assigned_at=now.replace(minute=2, second=0, microsecond=0),
            finished_at=now.replace(minute=3, second=0, microsecond=0),
            dispatch_meta_json={"remote_status": "success"},
        )
        unresolved_run = TaskRun(
            id="c" * 32,
            task_id=task_unresolved.id,
            orchestrator_id=orchestrator.id,
            status="success",
            assigned_at=now.replace(minute=4, second=0, microsecond=0),
            finished_at=now.replace(minute=5, second=0, microsecond=0),
            dispatch_meta_json={"remote_status": "success"},
        )
        session.add_all([resolved_old_run, resolved_new_run, unresolved_run])
        session.commit()

        session.add_all(
            [
                RunArtifact(
                    run_id=resolved_old_run.id,
                    source="output_gz",
                    parser_name="fixprice",
                    dataclass_validated=False,
                    dataclass_validation_error="old validation error",
                ),
                RunArtifact(
                    run_id=resolved_new_run.id,
                    source="output_gz",
                    parser_name="fixprice",
                    dataclass_validated=True,
                    dataclass_validation_error=None,
                ),
                RunArtifact(
                    run_id=unresolved_run.id,
                    source="output_gz",
                    parser_name="chizhik",
                    dataclass_validated=False,
                    dataclass_validation_error="missing required field: products",
                ),
            ]
        )
        session.commit()
    finally:
        session.close()

    with TestClient(app) as client:
        response = client.get("/api/validation-errors")
        assert response.status_code == 200
        body = response.json()
        assert body["total"] == 1
        assert len(body["items"]) == 1
        item = body["items"][0]
        assert item["task_id"] == task_unresolved.id
        assert item["run_id"] == "c" * 32
        assert item["store"] == "C902"
        assert item["parser_name"] == "chizhik"
        assert item["is_active"] is False
        assert "missing required field" in str(item["dataclass_validation_error"])


class _FakeParserSocket:
    def __init__(self) -> None:
        self.closed = False
        self.sent_payloads: list[str] = []
        self._responses = [
            json.dumps(
                {
                    "ok": True,
                    "action": "stream_job_log",
                    "event": "snapshot",
                    "job_id": "job-123",
                    "status": "running",
                    "lines": ["line-a", "line-b"],
                }
            ),
            json.dumps(
                {
                    "ok": True,
                    "action": "stream_job_log",
                    "event": "end",
                    "job_id": "job-123",
                    "status": "success",
                }
            ),
        ]

    async def send(self, payload: str) -> None:
        self.sent_payloads.append(payload)

    async def recv(self) -> str:
        assert self._responses, "No fake stream payloads left"
        return self._responses.pop(0)

    async def close(self) -> None:
        self.closed = True


def test_dashboard_run_log_proxy_ws(tmp_path: Path, monkeypatch):
    app = create_dashboard_app(_settings(tmp_path, ws_password="top-secret"))
    fake_socket = _FakeParserSocket()

    async def _fake_connect_orchestrator_ws(url: str):
        assert url == "ws://127.0.0.1:8765"
        return fake_socket

    monkeypatch.setattr("app.dashboard_app._connect_orchestrator_ws", _fake_connect_orchestrator_ws)

    session = app.state.session_factory()
    try:
        now = utcnow()
        task = CrawlTask(
            city="Moscow",
            store="C777",
            frequency_hours=24,
            parser_name="fixprice",
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        orchestrator = Orchestrator(
            id="o" * 32,
            name="parser-ws",
            token="t" * 40,
            created_at=now,
            updated_at=now,
            last_heartbeat_at=now,
        )
        session.add(task)
        session.add(orchestrator)
        session.commit()
        session.refresh(task)

        run = TaskRun(
            id="r" * 32,
            task_id=task.id,
            orchestrator_id=orchestrator.id,
            status="assigned",
            assigned_at=now,
            dispatch_meta_json={"remote_job_id": "job-123"},
        )
        session.add(run)
        session.commit()
    finally:
        session.close()

    with TestClient(app) as client:
        with client.websocket_connect("/ws/runs/" + ("r" * 32) + "/log?tail=2") as websocket:
            first_payload = websocket.receive_json()
            second_payload = websocket.receive_json()

        overview = client.get("/api/overview")
        assert overview.status_code == 200
        recent = overview.json()["recent_runs"][0]
        assert recent["remote_status"] is None
        assert recent["remote_terminal"] is False
        assert recent["can_open_live_log"] is True
        assert recent["display_status"] == "assigned"
        assert recent["validation_failed"] is False

    assert first_payload["event"] == "snapshot"
    assert first_payload["lines"] == ["line-a", "line-b"]
    assert second_payload["event"] == "end"
    assert fake_socket.closed is True

    sent_payload = json.loads(fake_socket.sent_payloads[0])
    assert sent_payload["action"] == "stream_job_log"
    assert sent_payload["job_id"] == "job-123"
    assert sent_payload["tail_lines"] == 2
    assert sent_payload["password"] == "top-secret"


def test_dashboard_overview_remote_terminal_disables_live_log(tmp_path: Path):
    app = create_dashboard_app(_settings(tmp_path))
    session = app.state.session_factory()
    try:
        now = utcnow()
        task = CrawlTask(
            city="Moscow",
            store="C778",
            frequency_hours=24,
            parser_name="fixprice",
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        orchestrator = Orchestrator(
            id="o" * 32,
            name="parser-ws",
            token="t" * 40,
            created_at=now,
            updated_at=now,
            last_heartbeat_at=now,
        )
        session.add(task)
        session.add(orchestrator)
        session.commit()
        session.refresh(task)

        run = TaskRun(
            id="z" * 32,
            task_id=task.id,
            orchestrator_id=orchestrator.id,
            status="assigned",
            assigned_at=now,
            dispatch_meta_json={"remote_job_id": "job-778", "remote_status": "success"},
        )
        session.add(run)
        session.commit()
    finally:
        session.close()

    with TestClient(app) as client:
        overview = client.get("/api/overview")
        assert overview.status_code == 200
        recent = overview.json()["recent_runs"][0]
        assert recent["status"] == "assigned"
        assert recent["display_status"] == "assigned"
        assert recent["remote_status"] == "success"
        assert recent["remote_terminal"] is True
        assert recent["can_open_live_log"] is False


def test_dashboard_overview_marks_validation_failed_as_warning(tmp_path: Path):
    app = create_dashboard_app(_settings(tmp_path))
    session = app.state.session_factory()
    try:
        now = utcnow()
        task = CrawlTask(
            city="Moscow",
            store="C779",
            frequency_hours=24,
            parser_name="fixprice",
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        orchestrator = Orchestrator(
            id="k" * 32,
            name="parser-ws-warning",
            token="u" * 40,
            created_at=now,
            updated_at=now,
            last_heartbeat_at=now,
        )
        run = TaskRun(
            id="w" * 32,
            task_id=1,
            orchestrator_id=orchestrator.id,
            status="success",
            assigned_at=now,
            finished_at=now,
            dispatch_meta_json={"remote_job_id": "job-779", "remote_status": "success"},
        )
        session.add(task)
        session.add(orchestrator)
        session.flush()
        run.task_id = task.id
        session.add(run)
        session.flush()
        session.add(
            RunArtifact(
                run_id=run.id,
                source="output_gz",
                parser_name="chizhik",
                dataclass_validated=False,
                dataclass_validation_error="validation failed",
            )
        )
        session.commit()
    finally:
        session.close()

    with TestClient(app) as client:
        overview = client.get("/api/overview")
        assert overview.status_code == 200
        body = overview.json()
        recent = body["recent_runs"][0]
        assert recent["status"] == "success"
        assert recent["display_status"] == "validation_failed"
        assert recent["validation_failed"] is True
        assert recent["dataclass_validated"] is False
        assert body["runs_warning"] >= 1
