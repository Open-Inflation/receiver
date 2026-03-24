from __future__ import annotations

from datetime import timedelta
import json
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import select

from app.config import Settings
from app.dashboard_app import create_dashboard_app
from app.models import CrawlTask, Orchestrator, ParserStoreDirectory, TaskRun
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
        assert "Компактный режим" in page.text
        assert "Город" not in page.text
        validation_page = client.get("/validation-errors")
        assert validation_page.status_code == 200
        assert "Validation Fix Queue" in validation_page.text

        create = client.post(
            "/api/tasks",
            json={
                "store": "C500",
                "frequency_hours": 24,
                "parser_name": "fixprice",
                "include_images": False,
                "use_product_info": False,
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
                "use_product_info": True,
                "is_active": False,
            },
        )
        assert update.status_code == 200
        assert update.json()["frequency_hours"] == 12
        assert update.json()["include_images"] is True
        assert update.json()["use_product_info"] is True
        assert update.json()["is_active"] is False

        tasks = client.get("/api/tasks")
        assert tasks.status_code == 200
        assert len(tasks.json()) == 1
        listed_task = tasks.json()[0]
        assert listed_task["include_images"] is True
        assert listed_task["use_product_info"] is True
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
            artifact_source="output_gz",
            artifact_dataclass_validated=False,
            artifact_dataclass_validation_error="old validation error",
            artifact_ingested_at=now.replace(minute=1, second=0, microsecond=0),
        )
        resolved_new_run = TaskRun(
            id="b" * 32,
            task_id=task_resolved.id,
            orchestrator_id=orchestrator.id,
            status="success",
            assigned_at=now.replace(minute=2, second=0, microsecond=0),
            finished_at=now.replace(minute=3, second=0, microsecond=0),
            dispatch_meta_json={"remote_status": "success"},
            artifact_source="output_gz",
            artifact_dataclass_validated=True,
            artifact_dataclass_validation_error=None,
            artifact_ingested_at=now.replace(minute=3, second=0, microsecond=0),
        )
        unresolved_run = TaskRun(
            id="c" * 32,
            task_id=task_unresolved.id,
            orchestrator_id=orchestrator.id,
            status="success",
            assigned_at=now.replace(minute=4, second=0, microsecond=0),
            finished_at=now.replace(minute=5, second=0, microsecond=0),
            dispatch_meta_json={"remote_status": "success"},
            artifact_source="output_gz",
            artifact_dataclass_validated=False,
            artifact_dataclass_validation_error="missing required field: products",
            artifact_ingested_at=now.replace(minute=5, second=0, microsecond=0),
        )
        session.add_all([resolved_old_run, resolved_new_run, unresolved_run])
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


class _FakeCancelSocket:
    def __init__(self) -> None:
        self.closed = False
        self.sent_payloads: list[str] = []
        self._responses = [
            json.dumps(
                {
                    "ok": True,
                    "action": "cancel_job",
                    "job_id": "job-321",
                    "status": "cancelled",
                }
            )
        ]

    async def send(self, payload: str) -> None:
        self.sent_payloads.append(payload)

    async def recv(self) -> str:
        assert self._responses, "No fake cancel payloads left"
        return self._responses.pop(0)

    async def close(self) -> None:
        self.closed = True


class _FakeForceRunSocket:
    def __init__(self) -> None:
        self.closed = False
        self.sent_payloads: list[str] = []
        self._responses = [
            json.dumps(
                {
                    "ok": True,
                    "action": "submit_store",
                    "job_id": "job-force-901",
                    "status": "running",
                }
            )
        ]

    async def send(self, payload: str) -> None:
        self.sent_payloads.append(payload)

    async def recv(self) -> str:
        assert self._responses, "No fake force-run payloads left"
        return self._responses.pop(0)

    async def close(self) -> None:
        self.closed = True


class _FakeStoreDirectorySyncSocket:
    def __init__(self, response_payload: dict[str, object]) -> None:
        self.closed = False
        self.sent_payloads: list[str] = []
        self._responses = [json.dumps(response_payload)]

    async def send(self, payload: str) -> None:
        self.sent_payloads.append(payload)

    async def recv(self) -> str:
        assert self._responses, "No fake store-directory payloads left"
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
        assert recent["converter_elapsed_sec"] == 0
        assert recent["finish"] is None

    assert first_payload["event"] == "snapshot"
    assert first_payload["lines"] == ["line-a", "line-b"]
    assert second_payload["event"] == "end"
    assert fake_socket.closed is True

    sent_payload = json.loads(fake_socket.sent_payloads[0])
    assert sent_payload["action"] == "stream_job_log"
    assert sent_payload["job_id"] == "job-123"
    assert sent_payload["tail_lines"] == 2
    assert sent_payload["password"] == "top-secret"


def test_dashboard_cancel_run_marks_local_status_and_overview(tmp_path: Path, monkeypatch):
    app = create_dashboard_app(_settings(tmp_path, ws_password="top-secret"))
    fake_socket = _FakeCancelSocket()

    async def _fake_connect_orchestrator_ws(url: str):
        assert url == "ws://127.0.0.1:8765"
        return fake_socket

    monkeypatch.setattr("app.dashboard_app._connect_orchestrator_ws", _fake_connect_orchestrator_ws)

    session = app.state.session_factory()
    try:
        now = utcnow()
        task = CrawlTask(
            city="Moscow",
            store="C880",
            frequency_hours=24,
            parser_name="fixprice",
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        orchestrator = Orchestrator(
            id="q" * 32,
            name="parser-ws-cancel",
            token="s" * 40,
            created_at=now,
            updated_at=now,
            last_heartbeat_at=now,
        )
        session.add(task)
        session.add(orchestrator)
        session.commit()
        session.refresh(task)

        run = TaskRun(
            id="m" * 32,
            task_id=task.id,
            orchestrator_id=orchestrator.id,
            status="assigned",
            assigned_at=now,
            dispatch_meta_json={"remote_job_id": "job-321", "remote_status": "running"},
        )
        session.add(run)
        session.commit()
    finally:
        session.close()

    with TestClient(app) as client:
        cancel = client.post("/api/runs/" + ("m" * 32) + "/cancel")
        assert cancel.status_code == 200
        cancel_body = cancel.json()
        assert cancel_body["ok"] is True
        assert cancel_body["status"] == "cancelled"

        overview = client.get("/api/overview")
        assert overview.status_code == 200
        recent = overview.json()["recent_runs"][0]
        assert recent["status"] == "error"
        assert recent["display_status"] == "cancelled"
        assert recent["remote_status"] == "cancelled"
        assert recent["remote_terminal"] is True
        assert recent["can_open_live_log"] is False
        assert recent["error_message"] == "Cancelled from dashboard"

    sent_payload = json.loads(fake_socket.sent_payloads[0])
    assert sent_payload["action"] == "cancel_job"
    assert sent_payload["job_id"] == "job-321"
    assert sent_payload["password"] == "top-secret"
    assert fake_socket.closed is True


def test_dashboard_cancel_run_requires_remote_job_link(tmp_path: Path):
    app = create_dashboard_app(_settings(tmp_path))
    session = app.state.session_factory()
    try:
        now = utcnow()
        task = CrawlTask(
            city="Moscow",
            store="C881",
            frequency_hours=24,
            parser_name="fixprice",
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        orchestrator = Orchestrator(
            id="v" * 32,
            name="parser-ws-cancel-no-link",
            token="w" * 40,
            created_at=now,
            updated_at=now,
            last_heartbeat_at=now,
        )
        run = TaskRun(
            id="n" * 32,
            task_id=1,
            orchestrator_id=orchestrator.id,
            status="assigned",
            assigned_at=now,
            dispatch_meta_json={},
        )
        session.add(task)
        session.add(orchestrator)
        session.flush()
        run.task_id = task.id
        session.add(run)
        session.commit()
    finally:
        session.close()

    with TestClient(app) as client:
        cancel = client.post("/api/runs/" + ("n" * 32) + "/cancel")
        assert cancel.status_code == 409


def test_dashboard_force_run_submits_and_creates_assigned_run(tmp_path: Path, monkeypatch):
    app = create_dashboard_app(_settings(tmp_path, ws_password="top-secret"))
    fake_socket = _FakeForceRunSocket()

    async def _fake_connect_orchestrator_ws(url: str):
        assert url == "ws://127.0.0.1:8765"
        return fake_socket

    monkeypatch.setattr("app.dashboard_app._connect_orchestrator_ws", _fake_connect_orchestrator_ws)

    session = app.state.session_factory()
    try:
        now = utcnow()
        task = CrawlTask(
            city="Moscow",
            store="C991",
            frequency_hours=24,
            parser_name="chizhik",
            include_images=False,
            use_product_info=False,
            is_active=False,
            created_at=now,
            updated_at=now,
        )
        session.add(task)
        session.commit()
        session.refresh(task)
        task_id = task.id
    finally:
        session.close()

    with TestClient(app) as client:
        force_run = client.post(f"/api/tasks/{task_id}/force-run")
        assert force_run.status_code == 200
        force_body = force_run.json()
        assert force_body["ok"] is True
        assert force_body["task_id"] == task_id
        assert force_body["status"] == "submitted"
        assert force_body["remote_job_id"] == "job-force-901"
        run_id = force_body["run_id"]

        overview = client.get("/api/overview")
        assert overview.status_code == 200
        recent = overview.json()["recent_runs"][0]
        assert recent["id"] == run_id
        assert recent["task_id"] == task_id
        assert recent["status"] == "assigned"
        assert recent["display_status"] == "assigned"
        assert recent["remote_status"] == "running"
        assert recent["remote_terminal"] is False
        assert recent["can_open_live_log"] is True

    sent_payload = json.loads(fake_socket.sent_payloads[0])
    assert sent_payload["action"] == "submit_store"
    assert sent_payload["store_code"] == "C991"
    assert sent_payload["parser"] == "chizhik"
    assert sent_payload["include_images"] is False
    assert sent_payload["use_product_info"] is False
    assert sent_payload["full_catalog"] is True
    assert sent_payload["password"] == "top-secret"
    assert fake_socket.closed is True


def test_dashboard_reset_last_crawl_sets_now(tmp_path: Path):
    app = create_dashboard_app(_settings(tmp_path))
    session = app.state.session_factory()
    try:
        now = utcnow()
        task = CrawlTask(
            city="Moscow",
            store="C992",
            frequency_hours=24,
            parser_name="fixprice",
            is_active=True,
            last_crawl_at=now - timedelta(days=3),
            created_at=now,
            updated_at=now,
        )
        session.add(task)
        session.commit()
        session.refresh(task)
        task_id = task.id
        previous_last_crawl_at = task.last_crawl_at
    finally:
        session.close()

    with TestClient(app) as client:
        reset = client.post(f"/api/tasks/{task_id}/reset-last-crawl")
        assert reset.status_code == 200
        reset_body = reset.json()
        assert reset_body["id"] == task_id
        assert reset_body["last_crawl_at"] is not None
        assert reset_body["is_due"] is False

        tasks = client.get("/api/tasks")
        assert tasks.status_code == 200
        listed = tasks.json()[0]
        assert listed["id"] == task_id
        assert listed["last_crawl_at"] is not None
        assert listed["is_due"] is False

    session = app.state.session_factory()
    try:
        task = session.get(CrawlTask, task_id)
        assert task is not None
        assert task.last_crawl_at is not None
        assert previous_last_crawl_at is not None
        assert task.last_crawl_at > previous_last_crawl_at
    finally:
        session.close()


def test_dashboard_store_directory_sync_upserts_and_deactivates(tmp_path: Path, monkeypatch):
    app = create_dashboard_app(_settings(tmp_path, ws_password="top-secret"))
    fake_socket = _FakeStoreDirectorySyncSocket(
        {
            "ok": True,
            "action": "collect_stores",
            "parser": "fixprice",
            "stores_count": 2,
            "stores": [
                {
                    "code": "C001",
                    "address": "Москва, Тестовая 1",
                    "longitude": 37.62,
                    "latitude": 55.75,
                    "administrative_unit": {
                        "name": "Москва",
                        "alias": "moskva",
                        "country": "RUS",
                        "region": "Москва",
                    },
                },
                {
                    "code": "C002",
                    "address": "Москва, Тестовая 2",
                    "longitude": 37.63,
                    "latitude": 55.76,
                    "administrative_unit": {
                        "name": "Москва",
                        "alias": "moskva",
                        "country": "RUS",
                        "region": "Москва",
                    },
                },
            ],
            "warnings": [],
        }
    )

    async def _fake_connect_orchestrator_ws(url: str):
        assert url == "ws://127.0.0.1:8765"
        return fake_socket

    monkeypatch.setattr("app.dashboard_app._connect_orchestrator_ws", _fake_connect_orchestrator_ws)

    session = app.state.session_factory()
    try:
        now = utcnow()
        session.add_all(
            [
                ParserStoreDirectory(
                    parser_name="fixprice",
                    store_code="C001",
                    city_name="Moscow old",
                    city_alias="old",
                    is_partial=True,
                    is_active=True,
                    first_seen_at=now,
                    last_seen_at=now,
                    updated_at=now,
                ),
                ParserStoreDirectory(
                    parser_name="fixprice",
                    store_code="COLD",
                    city_name="Cold city",
                    is_partial=True,
                    is_active=True,
                    first_seen_at=now,
                    last_seen_at=now,
                    updated_at=now,
                ),
            ]
        )
        session.commit()
    finally:
        session.close()

    with TestClient(app) as client:
        sync_resp = client.post(
            "/api/store-directory/sync",
            json={"parser_name": "fixprice"},
        )
        assert sync_resp.status_code == 200
        body = sync_resp.json()
        assert body["ok"] is True
        assert body["processed_count"] == 2
        assert body["inserted_count"] == 1
        assert body["updated_count"] == 1
        assert body["deactivated_count"] == 1
        assert body["active_before"] == 2
        assert body["active_after"] == 2

        active_rows = client.get("/api/store-directory", params={"parser_name": "fixprice"})
        assert active_rows.status_code == 200
        active_codes = {item["store_code"] for item in active_rows.json()}
        assert active_codes == {"C001", "C002"}

        all_rows = client.get(
            "/api/store-directory",
            params={"parser_name": "fixprice", "active_only": "false"},
        )
        assert all_rows.status_code == 200
        rows_by_code = {item["store_code"]: item for item in all_rows.json()}
        assert rows_by_code["COLD"]["is_active"] is False
        assert rows_by_code["C001"]["city_alias"] == "moskva"

    sent_payload = json.loads(fake_socket.sent_payloads[0])
    assert sent_payload["action"] == "collect_stores"
    assert sent_payload["parser"] == "fixprice"
    assert sent_payload["password"] == "top-secret"
    assert fake_socket.closed is True


def test_dashboard_store_directory_sync_empty_snapshot_keeps_active(tmp_path: Path, monkeypatch):
    app = create_dashboard_app(_settings(tmp_path))
    fake_socket = _FakeStoreDirectorySyncSocket(
        {
            "ok": True,
            "action": "collect_stores",
            "parser": "fixprice",
            "stores_count": 0,
            "stores": [],
            "warnings": [],
        }
    )

    async def _fake_connect_orchestrator_ws(url: str):
        assert url == "ws://127.0.0.1:8765"
        return fake_socket

    monkeypatch.setattr("app.dashboard_app._connect_orchestrator_ws", _fake_connect_orchestrator_ws)

    session = app.state.session_factory()
    try:
        now = utcnow()
        session.add(
            ParserStoreDirectory(
                parser_name="fixprice",
                store_code="C777",
                city_name="Москва",
                is_active=True,
                is_partial=True,
                first_seen_at=now,
                last_seen_at=now,
                updated_at=now,
            )
        )
        session.commit()
    finally:
        session.close()

    with TestClient(app) as client:
        sync_resp = client.post(
            "/api/store-directory/sync",
            json={"parser_name": "fixprice"},
        )
        assert sync_resp.status_code == 200
        body = sync_resp.json()
        assert body["processed_count"] == 0
        assert body["deactivated_count"] == 0
        assert body["active_before"] == 1
        assert body["active_after"] == 1
        assert any("empty store snapshot" in item.lower() for item in body["warnings"])

    session = app.state.session_factory()
    try:
        row = session.scalar(
            select(ParserStoreDirectory).where(
                ParserStoreDirectory.parser_name == "fixprice",
                ParserStoreDirectory.store_code == "C777",
            )
        )
        assert row is not None
        assert row.is_active is True
    finally:
        session.close()


def test_dashboard_store_directory_sync_keeps_partial_chizhik_rows(tmp_path: Path, monkeypatch):
    app = create_dashboard_app(_settings(tmp_path))
    fake_socket = _FakeStoreDirectorySyncSocket(
        {
            "ok": True,
            "action": "collect_stores",
            "parser": "chizhik",
            "stores_count": 1,
            "stores": [
                {
                    "code": "moskva",
                    "address": None,
                    "longitude": 37.62,
                    "latitude": 55.75,
                    "administrative_unit": {
                        "name": "Москва",
                        "alias": "moskva",
                    },
                }
            ],
            "warnings": ["partial virtual stores"],
        }
    )

    async def _fake_connect_orchestrator_ws(url: str):
        assert url == "ws://127.0.0.1:8765"
        return fake_socket

    monkeypatch.setattr("app.dashboard_app._connect_orchestrator_ws", _fake_connect_orchestrator_ws)

    with TestClient(app) as client:
        sync_resp = client.post(
            "/api/store-directory/sync",
            json={"parser_name": "chizhik"},
        )
        assert sync_resp.status_code == 200
        body = sync_resp.json()
        assert body["processed_count"] == 1
        assert body["partial_count"] == 1

        rows = client.get("/api/store-directory", params={"parser_name": "chizhik"})
        assert rows.status_code == 200
        listed = rows.json()
        assert len(listed) == 1
        assert listed[0]["store_code"] == "moskva"
        assert listed[0]["is_partial"] is True


def test_dashboard_store_directory_list_filters_parser_and_active_flag(tmp_path: Path):
    app = create_dashboard_app(_settings(tmp_path))

    session = app.state.session_factory()
    try:
        now = utcnow()
        session.add_all(
            [
                ParserStoreDirectory(
                    parser_name="fixprice",
                    store_code="C001",
                    city_name="Москва",
                    is_active=True,
                    is_partial=False,
                    first_seen_at=now,
                    last_seen_at=now,
                    updated_at=now,
                ),
                ParserStoreDirectory(
                    parser_name="fixprice",
                    store_code="C002",
                    city_name="Москва",
                    is_active=False,
                    is_partial=False,
                    first_seen_at=now,
                    last_seen_at=now,
                    updated_at=now,
                ),
                ParserStoreDirectory(
                    parser_name="chizhik",
                    store_code="moskva",
                    city_name="Москва",
                    is_active=True,
                    is_partial=True,
                    first_seen_at=now,
                    last_seen_at=now,
                    updated_at=now,
                ),
            ]
        )
        session.commit()
    finally:
        session.close()

    with TestClient(app) as client:
        fixprice_active = client.get("/api/store-directory", params={"parser_name": "fixprice"})
        assert fixprice_active.status_code == 200
        assert [item["store_code"] for item in fixprice_active.json()] == ["C001"]

        fixprice_all = client.get(
            "/api/store-directory",
            params={"parser_name": "fixprice", "active_only": "false"},
        )
        assert fixprice_all.status_code == 200
        assert {item["store_code"] for item in fixprice_all.json()} == {"C001", "C002"}

        chizhik_active = client.get("/api/store-directory", params={"parser_name": "chizhik"})
        assert chizhik_active.status_code == 200
        listed = chizhik_active.json()
        assert len(listed) == 1
        assert listed[0]["store_code"] == "moskva"


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


def test_dashboard_overview_exposes_category_progress(tmp_path: Path):
    app = create_dashboard_app(_settings(tmp_path))
    session = app.state.session_factory()
    try:
        now = utcnow()
        task = CrawlTask(
            city="Moscow",
            store="C780",
            frequency_hours=24,
            parser_name="fixprice",
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        orchestrator = Orchestrator(
            id="p" * 32,
            name="parser-ws-progress",
            token="x" * 40,
            created_at=now,
            updated_at=now,
            last_heartbeat_at=now,
        )
        run = TaskRun(
            id="y" * 32,
            task_id=1,
            orchestrator_id=orchestrator.id,
            status="assigned",
            assigned_at=now,
            dispatch_meta_json={
                "remote_job_id": "job-781",
                "remote_status": "running",
                "category_progress": {
                    "categories_total": 12,
                    "categories_done": 5,
                    "current_category_alias": "beverages",
                    "updated_at": now.isoformat(),
                },
            },
        )
        session.add(task)
        session.add(orchestrator)
        session.flush()
        run.task_id = task.id
        session.add(run)
        session.commit()
    finally:
        session.close()

    with TestClient(app) as client:
        overview = client.get("/api/overview")
        assert overview.status_code == 200
        recent = overview.json()["recent_runs"][0]
        assert recent["status"] == "assigned"
        assert recent["remote_status"] == "running"
        assert recent["progress_total"] == 12
        assert recent["progress_done"] == 5
        assert recent["progress_category_alias"] == "beverages"
        assert recent["progress_percent"] == 42


def test_dashboard_overview_exposes_finalize_error_message(tmp_path: Path):
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
            id="e" * 32,
            name="parser-ws-error",
            token="v" * 40,
            created_at=now,
            updated_at=now,
            last_heartbeat_at=now,
        )
        run = TaskRun(
            id="x" * 32,
            task_id=1,
            orchestrator_id=orchestrator.id,
            status="error",
            assigned_at=now,
            finished_at=now,
            error_message=(
                "Finalize failed: JSON member meta.json is too large "
                "(53746962 bytes), limit is 16777216"
            ),
            dispatch_meta_json={"remote_job_id": "job-780", "remote_status": "success"},
        )
        session.add(task)
        session.add(orchestrator)
        session.flush()
        run.task_id = task.id
        session.add(run)
        session.commit()
    finally:
        session.close()

    with TestClient(app) as client:
        overview = client.get("/api/overview")
        assert overview.status_code == 200
        recent = overview.json()["recent_runs"][0]
        assert recent["status"] == "error"
        assert recent["display_status"] == "error"
        assert recent["remote_status"] == "success"
        assert recent["error_message"] is not None
        assert "Finalize failed:" in recent["error_message"]
        assert "meta.json is too large" in recent["error_message"]


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
            artifact_source="output_gz",
            artifact_dataclass_validated=False,
            artifact_dataclass_validation_error="validation failed",
            artifact_ingested_at=now,
        )
        session.add(task)
        session.add(orchestrator)
        session.flush()
        run.task_id = task.id
        session.add(run)
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
