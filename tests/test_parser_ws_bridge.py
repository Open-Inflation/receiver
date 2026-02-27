from __future__ import annotations

import asyncio
import time

from sqlalchemy import func, select

from app.database import Base, create_session_factory, create_sqlalchemy_engine
from app.models import CrawlTask, Orchestrator, TaskRun
from app.services.parser_ws_bridge import ParserWsBridge
from app.services.scheduler import utcnow


class DummyParserBridge:
    def normalize_payload(self, payload):
        return {"normalized": payload}


class DummyImagePipeline:
    def __init__(self):
        self.archive_calls: list[str | None] = []

    def process_archive_images(self, archive_path):
        self.archive_calls.append(archive_path)
        return [
            {
                "filename": "shelf.webp",
                "uploaded_url": "http://storage.local/images/shelf.webp",
                "error": None,
            }
        ]


class DummyArtifactIngestor:
    def __init__(self):
        self.calls: list[tuple[str, dict[str, object]]] = []

    def ingest_run_output(self, session, *, run, **kwargs):
        self.calls.append((run.id, kwargs))
        return {"ok": True, "artifact_id": 1}


class FakePersistentSocket:
    def __init__(self):
        self.closed = False
        self.sent_payloads: list[str] = []
        self._responses = [
            '{"ok": true, "action": "pong"}',
            '{"ok": true, "action": "status"}',
        ]

    async def send(self, payload: str) -> None:
        self.sent_payloads.append(payload)

    async def recv(self) -> str:
        assert self._responses, "No fake WS responses left"
        return self._responses.pop(0)

    async def close(self) -> None:
        self.closed = True


class FakeSlowSocket(FakePersistentSocket):
    async def recv(self) -> str:
        await asyncio.sleep(0.2)
        return '{"ok": true, "action": "status"}'


class FakeWebsocketsModule:
    def __init__(self, socket: FakePersistentSocket):
        self._socket = socket
        self.connect_calls = 0

    async def connect(self, _url: str):
        self.connect_calls += 1
        return self._socket


async def _run_bridge_cycle_test(database_url: str) -> None:
    engine = create_sqlalchemy_engine(database_url)
    session_factory = create_session_factory(engine)
    Base.metadata.create_all(bind=engine)

    session = session_factory()
    try:
        task = CrawlTask(
            city="3",
            store="C001",
            frequency_hours=24,
            parser_name="fixprice",
            include_images=False,
            is_active=True,
            created_at=utcnow(),
            updated_at=utcnow(),
        )
        session.add(task)
        session.commit()
    finally:
        session.close()

    parser_bridge = DummyParserBridge()
    image_pipeline = DummyImagePipeline()
    artifact_ingestor = DummyArtifactIngestor()

    bridge = ParserWsBridge(
        session_factory=session_factory,
        parser_bridge=parser_bridge,
        image_pipeline=image_pipeline,
        artifact_ingestor=artifact_ingestor,
        lease_ttl_minutes=30,
        ws_url="ws://127.0.0.1:8765",
        ws_password=None,
        poll_interval_sec=0.2,
        manager_name="parser-ws-test",
        submit_include_images=True,
        submit_full_catalog=True,
        upload_archive_images=True,
    )

    responses = [
        {"ok": True, "action": "pong"},
        {"ok": True, "action": "submit_store", "job_id": "job-123", "status": "queued"},
        {"ok": True, "action": "pong"},
        {
            "ok": True,
            "action": "status",
            "job": {
                "job_id": "job-123",
                "status": "success",
                "output_json": "/tmp/result.json",
                "output_gz": "/tmp/result.tar.gz",
                "download_url": "http://127.0.0.1:8766/download?token=x",
                "download_sha256": "abc",
            },
        },
    ]
    ws_requests: list[dict[str, object]] = []

    async def fake_ws_request(payload):
        ws_requests.append(dict(payload))
        assert responses, f"Unexpected ws request: {payload}"
        return responses.pop(0)

    bridge._ws_request = fake_ws_request  # type: ignore[method-assign]

    await bridge.run_cycle()
    await bridge.run_cycle()

    check_session = session_factory()
    try:
        run = check_session.scalar(select(TaskRun).order_by(TaskRun.assigned_at.desc()))
        assert run is not None
        assert run.status == "success"
        assert run.processed_images == 1
        assert isinstance(run.dispatch_meta_json, dict)
        assert run.dispatch_meta_json.get("remote_job_id") == "job-123"

        task = check_session.get(CrawlTask, run.task_id)
        assert task is not None
        assert task.last_crawl_at is not None
    finally:
        check_session.close()
        engine.dispose()

    assert image_pipeline.archive_calls == ["/tmp/result.tar.gz"]
    assert len(artifact_ingestor.calls) == 1
    call_run_id, call_kwargs = artifact_ingestor.calls[0]
    assert isinstance(call_run_id, str)
    assert call_kwargs.get("output_gz") == "/tmp/result.tar.gz"
    assert ws_requests[1]["action"] == "submit_store"
    assert ws_requests[1]["full_catalog"] is True
    assert ws_requests[1]["include_images"] is False


def test_parser_ws_bridge_cycle(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'bridge.sqlite3'}"
    asyncio.run(_run_bridge_cycle_test(db_url))


async def _run_ws_persistent_connection_test() -> None:
    bridge = ParserWsBridge(
        session_factory=None,  # type: ignore[arg-type]
        parser_bridge=DummyParserBridge(),
        image_pipeline=DummyImagePipeline(),
        artifact_ingestor=DummyArtifactIngestor(),
        lease_ttl_minutes=30,
        ws_url="ws://127.0.0.1:8765",
        ws_password=None,
        poll_interval_sec=1.0,
        manager_name="parser-ws-test",
        submit_include_images=True,
        submit_full_catalog=True,
        upload_archive_images=True,
    )

    fake_socket = FakePersistentSocket()
    fake_ws_module = FakeWebsocketsModule(fake_socket)
    bridge._ws_module = fake_ws_module

    first = await bridge._ws_request({"action": "ping"})
    second = await bridge._ws_request({"action": "status"})

    assert first.get("ok") is True
    assert second.get("ok") is True
    assert fake_ws_module.connect_calls == 1
    assert len(fake_socket.sent_payloads) == 2

    await bridge.stop()
    assert fake_socket.closed is True


def test_ws_request_reuses_connection():
    asyncio.run(_run_ws_persistent_connection_test())


async def _run_ws_request_timeout_test() -> None:
    bridge = ParserWsBridge(
        session_factory=None,  # type: ignore[arg-type]
        parser_bridge=DummyParserBridge(),
        image_pipeline=DummyImagePipeline(),
        artifact_ingestor=DummyArtifactIngestor(),
        lease_ttl_minutes=30,
        ws_url="ws://127.0.0.1:8765",
        ws_password=None,
        poll_interval_sec=1.0,
        manager_name="parser-ws-test",
        submit_include_images=True,
        submit_full_catalog=True,
        upload_archive_images=True,
    )

    fake_socket = FakeSlowSocket()
    fake_ws_module = FakeWebsocketsModule(fake_socket)
    bridge._ws_module = fake_ws_module
    bridge._ws_request_timeout_sec = 0.05

    response = await bridge._ws_request({"action": "status"})
    assert response.get("ok") is False
    assert "timed out" in str(response.get("error", "")).lower()
    assert fake_ws_module.connect_calls >= 1

    await bridge.stop()


def test_ws_request_timeout_returns_error():
    asyncio.run(_run_ws_request_timeout_test())


async def _run_submit_fail_marks_run_error_test(database_url: str) -> None:
    engine = create_sqlalchemy_engine(database_url)
    session_factory = create_session_factory(engine)
    Base.metadata.create_all(bind=engine)

    session = session_factory()
    try:
        session.add(
            CrawlTask(
                city="3",
                store="C010",
                frequency_hours=24,
                parser_name="fixprice",
                is_active=True,
                created_at=utcnow(),
                updated_at=utcnow(),
            )
        )
        session.commit()
    finally:
        session.close()

    bridge = ParserWsBridge(
        session_factory=session_factory,
        parser_bridge=DummyParserBridge(),
        image_pipeline=DummyImagePipeline(),
        artifact_ingestor=DummyArtifactIngestor(),
        lease_ttl_minutes=30,
        ws_url="ws://127.0.0.1:8765",
        ws_password=None,
        poll_interval_sec=0.2,
        manager_name="parser-ws-submit-fail",
        submit_include_images=True,
        submit_full_catalog=True,
        upload_archive_images=True,
        max_claims_per_cycle=1,
    )

    responses = [
        {"ok": True, "action": "pong"},
        {"ok": False, "error": "submit unavailable"},
    ]

    async def fake_ws_request(payload):
        assert responses, f"Unexpected ws request: {payload}"
        return responses.pop(0)

    bridge._ws_request = fake_ws_request  # type: ignore[method-assign]
    await bridge.run_cycle()

    check_session = session_factory()
    try:
        run = check_session.scalar(select(TaskRun).order_by(TaskRun.assigned_at.desc()))
        assert run is not None
        assert run.status == "error"
        assert "Submit failed" in str(run.error_message)
        assert isinstance(run.dispatch_meta_json, dict)
        assert run.dispatch_meta_json.get("submit_error") == "submit unavailable"
        assert run.dispatch_meta_json.get("failed_at")

        task = check_session.get(CrawlTask, run.task_id)
        assert task is not None
        assert task.lease_owner_id is None
        assert task.lease_until is None
    finally:
        check_session.close()
        engine.dispose()


def test_submit_fail_marks_run_error(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'bridge-submit-fail.sqlite3'}"
    asyncio.run(_run_submit_fail_marks_run_error_test(db_url))


async def _run_status_fail_marks_run_error_test(database_url: str) -> None:
    engine = create_sqlalchemy_engine(database_url)
    session_factory = create_session_factory(engine)
    Base.metadata.create_all(bind=engine)

    session = session_factory()
    try:
        session.add(
            CrawlTask(
                city="3",
                store="C020",
                frequency_hours=24,
                parser_name="fixprice",
                is_active=True,
                created_at=utcnow(),
                updated_at=utcnow(),
            )
        )
        session.commit()
    finally:
        session.close()

    bridge = ParserWsBridge(
        session_factory=session_factory,
        parser_bridge=DummyParserBridge(),
        image_pipeline=DummyImagePipeline(),
        artifact_ingestor=DummyArtifactIngestor(),
        lease_ttl_minutes=30,
        ws_url="ws://127.0.0.1:8765",
        ws_password=None,
        poll_interval_sec=0.2,
        manager_name="parser-ws-status-fail",
        submit_include_images=True,
        submit_full_catalog=True,
        upload_archive_images=True,
        max_claims_per_cycle=1,
    )

    responses = [
        {"ok": True, "action": "pong"},
        {"ok": True, "action": "submit_store", "job_id": "job-020", "status": "queued"},
        {"ok": True, "action": "pong"},
        {"ok": False, "error": "status timeout"},
    ]

    async def fake_ws_request(payload):
        assert responses, f"Unexpected ws request: {payload}"
        return responses.pop(0)

    bridge._ws_request = fake_ws_request  # type: ignore[method-assign]
    await bridge.run_cycle()

    pause_session = session_factory()
    try:
        task = pause_session.scalar(select(CrawlTask).where(CrawlTask.store == "C020"))
        assert task is not None
        task.is_active = False
        task.updated_at = utcnow()
        pause_session.commit()
    finally:
        pause_session.close()

    await bridge.run_cycle()

    check_session = session_factory()
    try:
        run = check_session.scalar(select(TaskRun).order_by(TaskRun.assigned_at.desc()))
        assert run is not None
        assert run.status == "error"
        assert "Status request failed" in str(run.error_message)
        assert isinstance(run.dispatch_meta_json, dict)
        assert run.dispatch_meta_json.get("last_status_error") == "status timeout"
        assert run.dispatch_meta_json.get("failed_at")
    finally:
        check_session.close()
        engine.dispose()


def test_status_fail_marks_run_error(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'bridge-status-fail.sqlite3'}"
    asyncio.run(_run_status_fail_marks_run_error_test(db_url))


class FailingArtifactIngestor(DummyArtifactIngestor):
    def ingest_run_output(self, session, *, run, **kwargs):
        self.calls.append((run.id, kwargs))
        return {"ok": False, "error": "ingest failed"}


async def _run_finalize_fail_marks_run_error_test(database_url: str) -> None:
    engine = create_sqlalchemy_engine(database_url)
    session_factory = create_session_factory(engine)
    Base.metadata.create_all(bind=engine)

    session = session_factory()
    try:
        session.add(
            CrawlTask(
                city="3",
                store="C030",
                frequency_hours=24,
                parser_name="fixprice",
                is_active=True,
                created_at=utcnow(),
                updated_at=utcnow(),
            )
        )
        session.commit()
    finally:
        session.close()

    bridge = ParserWsBridge(
        session_factory=session_factory,
        parser_bridge=DummyParserBridge(),
        image_pipeline=DummyImagePipeline(),
        artifact_ingestor=FailingArtifactIngestor(),
        lease_ttl_minutes=30,
        ws_url="ws://127.0.0.1:8765",
        ws_password=None,
        poll_interval_sec=0.2,
        manager_name="parser-ws-finalize-fail",
        submit_include_images=True,
        submit_full_catalog=True,
        upload_archive_images=True,
        max_claims_per_cycle=1,
    )

    responses = [
        {"ok": True, "action": "pong"},
        {"ok": True, "action": "submit_store", "job_id": "job-030", "status": "queued"},
        {"ok": True, "action": "pong"},
        {
            "ok": True,
            "action": "status",
            "job": {
                "job_id": "job-030",
                "status": "success",
                "output_json": "/tmp/result.json",
                "output_gz": "/tmp/result.tar.gz",
            },
        },
    ]

    async def fake_ws_request(payload):
        assert responses, f"Unexpected ws request: {payload}"
        return responses.pop(0)

    bridge._ws_request = fake_ws_request  # type: ignore[method-assign]
    await bridge.run_cycle()

    pause_session = session_factory()
    try:
        task = pause_session.scalar(select(CrawlTask).where(CrawlTask.store == "C030"))
        assert task is not None
        task.is_active = False
        task.updated_at = utcnow()
        pause_session.commit()
    finally:
        pause_session.close()

    await bridge.run_cycle()

    check_session = session_factory()
    try:
        run = check_session.scalar(select(TaskRun).order_by(TaskRun.assigned_at.desc()))
        assert run is not None
        assert run.status == "error"
        assert "Finalize failed" in str(run.error_message)
        assert isinstance(run.dispatch_meta_json, dict)
        assert run.dispatch_meta_json.get("last_finalize_error")
        assert run.dispatch_meta_json.get("failed_at")
    finally:
        check_session.close()
        engine.dispose()


def test_finalize_exception_marks_run_error(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'bridge-finalize-fail.sqlite3'}"
    asyncio.run(_run_finalize_fail_marks_run_error_test(db_url))


async def _run_assigned_parallel_processing_test(database_url: str) -> None:
    engine = create_sqlalchemy_engine(database_url)
    session_factory = create_session_factory(engine)
    Base.metadata.create_all(bind=engine)

    session = session_factory()
    try:
        now = utcnow()
        orchestrator = Orchestrator(
            id="o" * 32,
            name="parser-ws-parallel",
            token="t" * 40,
            created_at=now,
            updated_at=now,
            last_heartbeat_at=now,
        )
        task1 = CrawlTask(
            city="3",
            store="C041",
            frequency_hours=24,
            parser_name="fixprice",
            is_active=True,
            created_at=now,
            updated_at=now,
            last_crawl_at=now,
        )
        task2 = CrawlTask(
            city="3",
            store="C042",
            frequency_hours=24,
            parser_name="fixprice",
            is_active=True,
            created_at=now,
            updated_at=now,
            last_crawl_at=now,
        )
        session.add_all([orchestrator, task1, task2])
        session.commit()
        session.refresh(task1)
        session.refresh(task2)

        session.add_all(
            [
                TaskRun(
                    id="r" * 32,
                    task_id=task1.id,
                    orchestrator_id=orchestrator.id,
                    status="assigned",
                    assigned_at=now,
                    dispatch_meta_json={"remote_job_id": "job-041"},
                ),
                TaskRun(
                    id="s" * 32,
                    task_id=task2.id,
                    orchestrator_id=orchestrator.id,
                    status="assigned",
                    assigned_at=now,
                    dispatch_meta_json={"remote_job_id": "job-042"},
                ),
            ]
        )
        session.commit()
    finally:
        session.close()

    bridge = ParserWsBridge(
        session_factory=session_factory,
        parser_bridge=DummyParserBridge(),
        image_pipeline=DummyImagePipeline(),
        artifact_ingestor=DummyArtifactIngestor(),
        lease_ttl_minutes=30,
        ws_url="ws://127.0.0.1:8765",
        ws_password=None,
        poll_interval_sec=0.2,
        manager_name="parser-ws-parallel",
        submit_include_images=True,
        submit_full_catalog=True,
        upload_archive_images=True,
        assigned_parallelism=2,
    )
    bridge._manager_orchestrator_id = "o" * 32

    async def fake_ws_request(payload):
        if payload.get("action") == "ping":
            return {"ok": True, "action": "pong"}
        if payload.get("action") == "status":
            return {
                "ok": True,
                "action": "status",
                "job": {
                    "job_id": payload.get("job_id"),
                    "status": "success",
                    "output_json": None,
                    "output_gz": None,
                },
            }
        raise AssertionError(f"Unexpected payload: {payload}")

    async def slow_archive(_archive_path):
        await asyncio.sleep(0.15)
        return []

    bridge._ws_request = fake_ws_request  # type: ignore[method-assign]
    bridge._process_archive_images = slow_archive  # type: ignore[method-assign]

    start = time.perf_counter()
    await bridge.run_cycle()
    elapsed = time.perf_counter() - start

    check_session = session_factory()
    try:
        runs = check_session.scalars(select(TaskRun).order_by(TaskRun.id.asc())).all()
        assert len(runs) == 2
        assert all(run.status == "success" for run in runs)
    finally:
        check_session.close()
        engine.dispose()

    assert elapsed < 0.28


def test_assigned_runs_processed_with_parallelism(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'bridge-parallel.sqlite3'}"
    asyncio.run(_run_assigned_parallel_processing_test(db_url))


async def _run_claim_cycle_cap_test(database_url: str) -> None:
    engine = create_sqlalchemy_engine(database_url)
    session_factory = create_session_factory(engine)
    Base.metadata.create_all(bind=engine)

    session = session_factory()
    try:
        now = utcnow()
        session.add_all(
            [
                CrawlTask(
                    city="3",
                    store="CAP-1",
                    frequency_hours=24,
                    parser_name="fixprice",
                    is_active=True,
                    created_at=now,
                    updated_at=now,
                ),
                CrawlTask(
                    city="3",
                    store="CAP-2",
                    frequency_hours=24,
                    parser_name="fixprice",
                    is_active=True,
                    created_at=now,
                    updated_at=now,
                ),
                CrawlTask(
                    city="3",
                    store="CAP-3",
                    frequency_hours=24,
                    parser_name="fixprice",
                    is_active=True,
                    created_at=now,
                    updated_at=now,
                ),
            ]
        )
        session.commit()
    finally:
        session.close()

    bridge = ParserWsBridge(
        session_factory=session_factory,
        parser_bridge=DummyParserBridge(),
        image_pipeline=DummyImagePipeline(),
        artifact_ingestor=DummyArtifactIngestor(),
        lease_ttl_minutes=30,
        ws_url="ws://127.0.0.1:8765",
        ws_password=None,
        poll_interval_sec=0.2,
        manager_name="parser-ws-claim-cap",
        submit_include_images=True,
        submit_full_catalog=True,
        upload_archive_images=True,
        max_claims_per_cycle=2,
    )

    submit_calls: list[dict[str, object]] = []

    async def fake_ws_request(payload):
        if payload.get("action") == "ping":
            return {"ok": True, "action": "pong"}
        if payload.get("action") == "submit_store":
            submit_calls.append(dict(payload))
            return {
                "ok": True,
                "action": "submit_store",
                "job_id": f"job-{len(submit_calls)}",
                "status": "queued",
            }
        raise AssertionError(f"Unexpected payload: {payload}")

    bridge._ws_request = fake_ws_request  # type: ignore[method-assign]
    await bridge.run_cycle()

    check_session = session_factory()
    try:
        assigned_count = check_session.scalar(select(func.count(TaskRun.id)).where(TaskRun.status == "assigned"))
        assert assigned_count == 2
    finally:
        check_session.close()
        engine.dispose()

    assert len(submit_calls) == 2


def test_claim_cycle_cap(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'bridge-claim-cap.sqlite3'}"
    asyncio.run(_run_claim_cycle_cap_test(db_url))
