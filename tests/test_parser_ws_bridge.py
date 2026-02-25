from __future__ import annotations

import asyncio
from datetime import datetime, timezone

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
        self.calls: list[str] = []

    def ingest_run_output(self, session, *, run):
        self.calls.append(run.id)
        return {"ok": True, "artifact_id": 1}


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
                "download_expires_at": datetime.now(timezone.utc).isoformat(),
            },
        },
    ]

    async def fake_ws_request(payload):
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
        assert run.output_gz == "/tmp/result.tar.gz"
        assert run.image_results_json is not None
        assert len(run.image_results_json) == 1

        task = check_session.get(CrawlTask, run.task_id)
        assert task is not None
        assert task.last_crawl_at is not None
    finally:
        check_session.close()
        engine.dispose()

    assert image_pipeline.archive_calls == ["/tmp/result.tar.gz"]
    assert len(artifact_ingestor.calls) == 1


def test_parser_ws_bridge_cycle(tmp_path):
    db_url = f"sqlite:///{tmp_path / 'bridge.sqlite3'}"
    asyncio.run(_run_bridge_cycle_test(db_url))
