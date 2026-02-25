from __future__ import annotations

from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, status
from sqlalchemy import select
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

from .config import Settings, load_settings
from .database import create_session_factory, create_sqlalchemy_engine
from .deps import get_current_orchestrator, get_db_session
from .models import Base, CrawlTask, Orchestrator, TaskRun
from .schemas import (
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
from .services.image_pipeline import ImagePipeline
from .services.parser_bridge import ParserBridge
from .services.scheduler import (
    TERMINAL_RUN_STATUSES,
    claim_next_due_task,
    create_or_get_orchestrator,
    finish_run,
    touch_orchestrator,
    utcnow,
)


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

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        try:
            yield
        finally:
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
    app.state.parser_bridge = ParserBridge(parser_src_path=app_settings.parser_src_path)
    app.state.image_pipeline = ImagePipeline(
        storage_base_url=app_settings.storage_base_url,
        storage_api_token=app_settings.storage_api_token,
    )

    @app.get("/healthz")
    def healthcheck() -> dict[str, object]:
        return {
            "status": "ok",
            "parser_integration": app.state.parser_bridge.enabled,
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
        return session.scalars(select(CrawlTask).order_by(CrawlTask.id.asc())).all()

    @app.patch("/api/tasks/{task_id}", response_model=TaskOut)
    def update_task(
        task_id: int,
        payload: TaskUpdate,
        session: Session = Depends(get_db_session),
    ) -> CrawlTask:
        task = session.get(CrawlTask, task_id)
        if task is None:
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
