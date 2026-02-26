from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import logging
from pathlib import Path
import sys
import time

from fastapi import Depends, FastAPI, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.config import Settings, load_settings
from app.database import create_session_factory, create_sqlalchemy_engine
from app.deps import get_current_orchestrator, get_db_session
from app.logging_utils import ensure_logging_configured
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
    ensure_logging_configured()
    app_settings = settings or load_settings()
    safe_database_url = make_url(app_settings.database_url).render_as_string(hide_password=True)
    LOGGER.info(
        "Initializing receiver app: db_url=%s ws_url=%s auto_dispatch=%s image_upload_parallelism=%s ws_request_timeout_sec=%.1f max_claims_per_cycle=%s assigned_parallelism=%s",
        safe_database_url,
        app_settings.orchestrator_ws_url,
        app_settings.orchestrator_auto_dispatch_enabled,
        app_settings.image_upload_parallelism,
        app_settings.orchestrator_ws_request_timeout_sec,
        app_settings.orchestrator_max_claims_per_cycle,
        app_settings.orchestrator_assigned_parallelism,
    )

    _ensure_sqlite_parent_dir(app_settings.database_url)
    engine = create_sqlalchemy_engine(app_settings.database_url)
    session_factory = create_session_factory(engine)
    Base.metadata.create_all(bind=engine)
    apply_compat_schema_patches(engine)

    parser_bridge = ParserBridge(parser_src_path=app_settings.parser_src_path)
    image_pipeline = ImagePipeline(
        storage_base_url=app_settings.storage_base_url,
        storage_api_token=app_settings.storage_api_token,
        max_parallel_uploads=app_settings.image_upload_parallelism,
        image_archive_max_file_bytes=app_settings.image_archive_max_file_bytes,
        image_archive_max_files=app_settings.image_archive_max_files,
    )
    artifact_ingestor = ArtifactIngestor(
        parser_src_path=app_settings.parser_src_path,
        download_max_bytes=app_settings.artifact_download_max_bytes,
        json_member_max_bytes=app_settings.artifact_json_member_max_bytes,
    )
    ws_bridge = ParserWsBridge(
        session_factory=session_factory,
        parser_bridge=parser_bridge,
        image_pipeline=image_pipeline,
        artifact_ingestor=artifact_ingestor,
        lease_ttl_minutes=app_settings.lease_ttl_minutes,
        ws_url=app_settings.orchestrator_ws_url,
        ws_password=app_settings.orchestrator_ws_password,
        poll_interval_sec=app_settings.orchestrator_poll_interval_sec,
        ws_request_timeout_sec=app_settings.orchestrator_ws_request_timeout_sec,
        max_claims_per_cycle=app_settings.orchestrator_max_claims_per_cycle,
        assigned_parallelism=app_settings.orchestrator_assigned_parallelism,
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
            LOGGER.info("Parser WS bridge task started")
        try:
            yield
        finally:
            if bridge_task is not None:
                LOGGER.info("Stopping parser WS bridge task")
                await ws_bridge.stop()
                await bridge_task
            LOGGER.info("Disposing SQLAlchemy engine")
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

    @app.middleware("http")
    async def log_http_requests(request: Request, call_next):
        start = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            elapsed_ms = (time.perf_counter() - start) * 1000.0
            LOGGER.exception(
                "HTTP %s %s failed in %.2fms",
                request.method,
                request.url.path,
                elapsed_ms,
            )
            raise

        elapsed_ms = (time.perf_counter() - start) * 1000.0
        LOGGER.info(
            "HTTP %s %s -> %s in %.2fms",
            request.method,
            request.url.path,
            response.status_code,
            elapsed_ms,
        )
        return response

    @app.get("/healthz")
    def healthcheck() -> dict[str, object]:
        LOGGER.debug("Healthcheck requested")
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
        orchestrator_name = payload.name.strip()
        orchestrator = create_or_get_orchestrator(session, name=orchestrator_name)
        LOGGER.info(
            "Orchestrator registered: id=%s name=%s",
            orchestrator.id,
            orchestrator_name,
        )
        return RegisterOrchestratorOut(orchestrator_id=orchestrator.id, token=orchestrator.token)

    @app.post("/api/orchestrators/heartbeat", response_model=HeartbeatOut)
    def orchestrator_heartbeat(
        orchestrator: Orchestrator = Depends(get_current_orchestrator),
        session: Session = Depends(get_db_session),
    ) -> HeartbeatOut:
        touch_orchestrator(session, orchestrator, commit=True)
        LOGGER.debug(
            "Heartbeat accepted: orchestrator_id=%s name=%s",
            orchestrator.id,
            orchestrator.name,
        )
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
        now = utcnow()
        task = CrawlTask(
            city=payload.city.strip(),
            store=payload.store.strip(),
            frequency_hours=payload.frequency_hours,
            last_crawl_at=payload.last_crawl_at,
            parser_name=payload.parser_name.strip(),
            is_active=payload.is_active,
            created_at=now,
            updated_at=now,
        )
        session.add(task)
        session.commit()
        session.refresh(task)
        LOGGER.info(
            "Task created: id=%s city=%s store=%s parser=%s active=%s frequency_hours=%s",
            task.id,
            task.city,
            task.store,
            task.parser_name,
            task.is_active,
            task.frequency_hours,
        )
        return task

    @app.get("/api/tasks", response_model=list[TaskOut])
    def list_tasks(session: Session = Depends(get_db_session)) -> list[CrawlTask]:
        tasks = session.scalars(
            select(CrawlTask).where(CrawlTask.deleted_at.is_(None)).order_by(CrawlTask.id.asc())
        ).all()
        LOGGER.debug("Tasks listed: count=%s", len(tasks))
        return tasks

    @app.patch("/api/tasks/{task_id}", response_model=TaskOut)
    def update_task(
        task_id: int,
        payload: TaskUpdate,
        session: Session = Depends(get_db_session),
    ) -> CrawlTask:
        task = session.get(CrawlTask, task_id)
        if task is None or task.deleted_at is not None:
            LOGGER.warning("Task update failed: task_id=%s reason=not_found", task_id)
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
        LOGGER.info(
            "Task updated: id=%s active=%s frequency_hours=%s parser=%s leased=%s",
            task.id,
            task.is_active,
            task.frequency_hours,
            task.parser_name,
            bool(task.lease_owner_id),
        )
        return task

    @app.delete("/api/tasks/{task_id}")
    def delete_task(
        task_id: int,
        session: Session = Depends(get_db_session),
    ) -> dict[str, object]:
        task = session.get(CrawlTask, task_id)
        if task is None or task.deleted_at is not None:
            LOGGER.warning("Task delete failed: task_id=%s reason=not_found", task_id)
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")

        now = utcnow()
        task.deleted_at = now
        task.is_active = False
        task.lease_owner_id = None
        task.lease_until = None
        task.updated_at = now
        session.commit()
        LOGGER.info("Task deleted: id=%s", task_id)
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
            LOGGER.debug("No due task for orchestrator_id=%s", orchestrator.id)
            return NextTaskOut(assignment=None)

        task, run = claimed
        LOGGER.info(
            "Task assigned: run_id=%s task_id=%s orchestrator_id=%s",
            run.id,
            task.id,
            orchestrator.id,
        )
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
            LOGGER.warning(
                "Run result rejected: run_id=%s orchestrator_id=%s reason=run_not_found",
                payload.run_id,
                orchestrator.id,
            )
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
        if run.orchestrator_id != orchestrator.id:
            LOGGER.warning(
                "Run result rejected: run_id=%s orchestrator_id=%s reason=ownership_mismatch owner_id=%s",
                payload.run_id,
                orchestrator.id,
                run.orchestrator_id,
            )
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Run belongs to another orchestrator")
        if run.status in TERMINAL_RUN_STATUSES:
            LOGGER.warning(
                "Run result rejected: run_id=%s orchestrator_id=%s reason=already_finished status=%s",
                payload.run_id,
                orchestrator.id,
                run.status,
            )
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Run already finished")

        parser_payload = app.state.parser_bridge.normalize_payload(payload.payload)
        image_results = app.state.image_pipeline.process_images(payload.images)
        if payload.upload_images_from_archive:
            image_results.extend(app.state.image_pipeline.process_archive_images(payload.output_gz))

        error_message = payload.error_message
        if payload.status == "error" and not error_message:
            error_message = "Orchestrator returned error status"

        processed_images = sum(1 for item in image_results if item.get("uploaded_url"))
        finished_run = finish_run(
            session,
            run=run,
            orchestrator=orchestrator,
            status=payload.status,
            processed_images=processed_images,
            error_message=error_message,
        )

        if payload.status == "success":
            try:
                ingest_result = app.state.artifact_ingestor.ingest_run_output(
                    session,
                    run=finished_run,
                    output_json=payload.output_json,
                    output_gz=payload.output_gz,
                    download_url=payload.download_url,
                    download_sha256=payload.download_sha256,
                    image_results=image_results,
                )
                if not ingest_result.get("ok"):
                    LOGGER.warning(
                        "Artifact ingest failed for run %s: %s",
                        finished_run.id,
                        ingest_result.get("error"),
                    )
            except Exception:
                LOGGER.exception("Artifact ingest crashed for run %s", finished_run.id)

        LOGGER.info(
            "Run result accepted: run_id=%s status=%s orchestrator_id=%s processed_images=%s",
            finished_run.id,
            finished_run.status,
            orchestrator.id,
            processed_images,
        )

        return SubmitResultOut(
            run_id=finished_run.id,
            status=finished_run.status,
            processed_images=processed_images,
            image_results=image_results,
            output_json=payload.output_json,
            output_gz=payload.output_gz,
            download_url=payload.download_url,
            download_sha256=payload.download_sha256,
            download_expires_at=payload.download_expires_at,
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
            LOGGER.warning("Run fetch failed: run_id=%s orchestrator_id=%s reason=not_found", run_id, orchestrator.id)
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
        if run.orchestrator_id != orchestrator.id:
            LOGGER.warning(
                "Run fetch forbidden: run_id=%s orchestrator_id=%s owner_id=%s",
                run_id,
                orchestrator.id,
                run.orchestrator_id,
            )
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Run belongs to another orchestrator")
        LOGGER.debug("Run fetched: run_id=%s orchestrator_id=%s status=%s", run.id, orchestrator.id, run.status)
        return run

    return app


app = create_app()


def main() -> None:
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8090)


if __name__ == "__main__":
    main()
