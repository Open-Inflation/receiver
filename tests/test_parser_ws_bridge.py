from __future__ import annotations

import asyncio

from sqlalchemy import select

from app.database import Base, create_session_factory, create_sqlalchemy_engine
from app.models import CrawlTask, TaskRun
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
    assert ws_requests[1]["include_images"] is True


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
