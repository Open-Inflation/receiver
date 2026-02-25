from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import logging
from pathlib import Path
import sys

from fastapi import Depends, FastAPI, HTTPException, status
from sqlalchemy import select
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import Settings, load_settings
from app.database import create_session_factory, create_sqlalchemy_engine
from app.deps import get_current_orchestrator, get_db_session
from app.models import Base, CrawlTask, Orchestrator, TaskRun
from app.schema_patch import apply_compat_schema_patches
from app.schemas import (
    AssignmentOut,
    HeartbeatOut,
    NextTaskOut,
    RegisterOrchestratorIn,
    RegisterOrchestratorOut,
    SubmitResultIn,
    SubmitResultOut,
    TaskCreate,
    TaskOut,
    TaskRunOut,
    TaskUpdate,
)
from app.services.image_pipeline import ImagePipeline
from app.services.artifact_ingestor import ArtifactIngestor
from app.services.parser_bridge import ParserBridge
from app.services.parser_ws_bridge import ParserWsBridge
from app.services.scheduler import (
    TERMINAL_RUN_STATUSES,
    claim_next_due_task,
    create_or_get_orchestrator,
    finish_run,
    touch_orchestrator,
    utcnow,
)

LOGGER = logging.getLogger(__name__)


def _ensure_sqlite_parent_dir(database_url: str) -> None:
    url = make_url(database_url)
    if not url.drivername.startswith("sqlite"):
        return

    if not url.database or url.database == ":memory:":
        return

    db_path = Path(url.database).expanduser()
    if not db_path.is_absolute():
        db_path = (Path.cwd() / db_path).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)


def create_app(settings: Settings | None = None) -> FastAPI:
    app_settings = settings or load_settings()

    _ensure_sqlite_parent_dir(app_settings.database_url)
    engine = create_sqlalchemy_engine(app_settings.database_url)
    session_factory = create_session_factory(engine)
    Base.metadata.create_all(bind=engine)
    apply_compat_schema_patches(engine)

    parser_bridge = ParserBridge(parser_src_path=app_settings.parser_src_path)
    image_pipeline = ImagePipeline(
        storage_base_url=app_settings.storage_base_url,
        storage_api_token=app_settings.storage_api_token,
    )
    artifact_ingestor = ArtifactIngestor(parser_src_path=app_settings.parser_src_path)
    ws_bridge = ParserWsBridge(
        session_factory=session_factory,
        parser_bridge=parser_bridge,
        image_pipeline=image_pipeline,
        artifact_ingestor=artifact_ingestor,
        lease_ttl_minutes=app_settings.lease_ttl_minutes,
        ws_url=app_settings.orchestrator_ws_url,
        ws_password=app_settings.orchestrator_ws_password,
        poll_interval_sec=app_settings.orchestrator_poll_interval_sec,
        manager_name=app_settings.orchestrator_manager_name,
        submit_include_images=app_settings.orchestrator_submit_include_images,
        submit_full_catalog=app_settings.orchestrator_submit_full_catalog,
        upload_archive_images=app_settings.orchestrator_upload_archive_images,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        bridge_task: asyncio.Task[None] | None = None
        if app_settings.orchestrator_auto_dispatch_enabled:
            bridge_task = asyncio.create_task(
                ws_bridge.run_forever(),
                name="receiver-parser-ws-bridge",
            )
        try:
            yield
        finally:
            if bridge_task is not None:
                await ws_bridge.stop()
                await bridge_task
            engine.dispose()

    app = FastAPI(
        title="Receiver Server",
        description="Task scheduler/receiver for orchestrators with parser and storage integration",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.state.settings = app_settings
    app.state.engine = engine
    app.state.session_factory = session_factory
    app.state.parser_bridge = parser_bridge
    app.state.image_pipeline = image_pipeline
    app.state.artifact_ingestor = artifact_ingestor
    app.state.parser_ws_bridge = ws_bridge

    @app.get("/healthz")
    def healthcheck() -> dict[str, object]:
        return {
            "status": "ok",
            "parser_integration": app.state.parser_bridge.enabled,
            "dataclass_integration": app.state.artifact_ingestor.dataclass_enabled,
            "orchestrator_bridge_enabled": app.state.settings.orchestrator_auto_dispatch_enabled,
            "orchestrator_ws_url": app.state.settings.orchestrator_ws_url,
        }

    @app.post("/api/orchestrators/register", response_model=RegisterOrchestratorOut)
    def register_orchestrator(
        payload: RegisterOrchestratorIn,
        session: Session = Depends(get_db_session),
    ) -> RegisterOrchestratorOut:
        orchestrator = create_or_get_orchestrator(session, name=payload.name.strip())
        return RegisterOrchestratorOut(orchestrator_id=orchestrator.id, token=orchestrator.token)

    @app.post("/api/orchestrators/heartbeat", response_model=HeartbeatOut)
    def orchestrator_heartbeat(
        orchestrator: Orchestrator = Depends(get_current_orchestrator),
        session: Session = Depends(get_db_session),
    ) -> HeartbeatOut:
        touch_orchestrator(session, orchestrator, commit=True)
        return HeartbeatOut(
            orchestrator_id=orchestrator.id,
            last_heartbeat_at=orchestrator.last_heartbeat_at,
        )

    @app.post(
        "/api/tasks",
        response_model=TaskOut,
        status_code=status.HTTP_201_CREATED,
    )
    def create_task(
        payload: TaskCreate,
        session: Session = Depends(get_db_session),
    ) -> CrawlTask:
        task = CrawlTask(
            city=payload.city.strip(),
            store=payload.store.strip(),
            frequency_hours=payload.frequency_hours,
            last_crawl_at=payload.last_crawl_at,
            parser_name=payload.parser_name.strip(),
            is_active=payload.is_active,
            created_at=utcnow(),
            updated_at=utcnow(),
        )
        session.add(task)
        session.commit()
        session.refresh(task)
        return task

    @app.get("/api/tasks", response_model=list[TaskOut])
    def list_tasks(session: Session = Depends(get_db_session)) -> list[CrawlTask]:
        return session.scalars(
            select(CrawlTask).where(CrawlTask.deleted_at.is_(None)).order_by(CrawlTask.id.asc())
        ).all()

    @app.patch("/api/tasks/{task_id}", response_model=TaskOut)
    def update_task(
        task_id: int,
        payload: TaskUpdate,
        session: Session = Depends(get_db_session),
    ) -> CrawlTask:
        task = session.get(CrawlTask, task_id)
        if task is None or task.deleted_at is not None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")

        changes = payload.model_dump(exclude_unset=True)
        for field_name, value in changes.items():
            setattr(task, field_name, value)

        if task.is_active is False:
            task.lease_owner_id = None
            task.lease_until = None

        task.updated_at = utcnow()
        session.commit()
        session.refresh(task)
        return task

    @app.delete("/api/tasks/{task_id}")
    def delete_task(
        task_id: int,
        session: Session = Depends(get_db_session),
    ) -> dict[str, object]:
        task = session.get(CrawlTask, task_id)
        if task is None or task.deleted_at is not None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")

        now = utcnow()
        task.deleted_at = now
        task.is_active = False
        task.lease_owner_id = None
        task.lease_until = None
        task.updated_at = now
        session.commit()
        return {"ok": True, "task_id": task_id}

    @app.post("/api/orchestrators/next-task", response_model=NextTaskOut)
    def get_next_task(
        orchestrator: Orchestrator = Depends(get_current_orchestrator),
        session: Session = Depends(get_db_session),
    ) -> NextTaskOut:
        touch_orchestrator(session, orchestrator, commit=False)

        claimed = claim_next_due_task(
            session,
            orchestrator=orchestrator,
            lease_ttl_minutes=app.state.settings.lease_ttl_minutes,
        )
        if claimed is None:
            session.commit()
            return NextTaskOut(assignment=None)

        task, run = claimed
        return NextTaskOut(
            assignment=AssignmentOut(
                run_id=run.id,
                task=TaskOut.model_validate(task),
            )
        )

    @app.post("/api/orchestrators/results", response_model=SubmitResultOut)
    def submit_result(
        payload: SubmitResultIn,
        orchestrator: Orchestrator = Depends(get_current_orchestrator),
        session: Session = Depends(get_db_session),
    ) -> SubmitResultOut:
        run = session.get(TaskRun, payload.run_id)
        if run is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
        if run.orchestrator_id != orchestrator.id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Run belongs to another orchestrator")
        if run.status in TERMINAL_RUN_STATUSES:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Run already finished")

        parser_payload = app.state.parser_bridge.normalize_payload(payload.payload)
        image_results = app.state.image_pipeline.process_images(payload.images)
        if payload.upload_images_from_archive:
            image_results.extend(app.state.image_pipeline.process_archive_images(payload.output_gz))

        error_message = payload.error_message
        if payload.status == "error" and not error_message:
            error_message = "Orchestrator returned error status"

        finished_run = finish_run(
            session,
            run=run,
            orchestrator=orchestrator,
            status=payload.status,
            payload=payload.payload,
            parser_payload=parser_payload,
            image_results=image_results,
            output_json=payload.output_json,
            output_gz=payload.output_gz,
            download_url=payload.download_url,
            download_sha256=payload.download_sha256,
            download_expires_at=payload.download_expires_at,
            error_message=error_message,
        )

        if payload.status == "success":
            try:
                ingest_result = app.state.artifact_ingestor.ingest_run_output(
                    session,
                    run=finished_run,
                )
                if not ingest_result.get("ok"):
                    LOGGER.warning(
                        "Artifact ingest failed for run %s: %s",
                        finished_run.id,
                        ingest_result.get("error"),
                    )
            except Exception:
                LOGGER.exception("Artifact ingest crashed for run %s", finished_run.id)

        processed_images = sum(1 for item in image_results if item.get("uploaded_url"))
        return SubmitResultOut(
            run_id=finished_run.id,
            status=finished_run.status,
            processed_images=processed_images,
            image_results=image_results,
            output_json=finished_run.output_json,
            output_gz=finished_run.output_gz,
            download_url=finished_run.download_url,
            download_sha256=finished_run.download_sha256,
            download_expires_at=finished_run.download_expires_at,
            parser_payload=parser_payload,
        )

    @app.get("/api/runs/{run_id}", response_model=TaskRunOut)
    def get_run(
        run_id: str,
        orchestrator: Orchestrator = Depends(get_current_orchestrator),
        session: Session = Depends(get_db_session),
    ) -> TaskRun:
        run = session.get(TaskRun, run_id)
        if run is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
        if run.orchestrator_id != orchestrator.id:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Run belongs to another orchestrator")
        return run

    return app


app = create_app()


def main() -> None:
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8090)


if __name__ == "__main__":
    main()
