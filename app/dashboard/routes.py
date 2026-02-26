from __future__ import annotations

import contextlib
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.config import Settings
from app.models import CrawlTask, Orchestrator, RunArtifact, TaskRun
from app.services.scheduler import as_utc, is_task_due, utcnow

from .schemas import TaskCreateIn, TaskUpdateIn
from .utils import dispatch_meta, task_to_dict

LOGGER = logging.getLogger(__name__)

OrchestratorWsConnector = Callable[[str], Awaitable[Any]]
OrchestratorWsConnectorGetter = Callable[[], OrchestratorWsConnector]
SessionFactory = Callable[[], Session]


def create_dashboard_router(
    *,
    session_factory: SessionFactory,
    app_settings: Settings,
    connect_orchestrator_ws_getter: OrchestratorWsConnectorGetter,
) -> APIRouter:
    router = APIRouter()

    @router.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @router.get("/api/tasks")
    def list_tasks() -> list[dict[str, object]]:
        session = session_factory()
        try:
            now = utcnow()
            tasks = session.scalars(
                select(CrawlTask).where(CrawlTask.deleted_at.is_(None)).order_by(CrawlTask.id.asc())
            ).all()
            LOGGER.debug("Dashboard tasks listed: count=%s", len(tasks))
            return [task_to_dict(task, now=now) for task in tasks]
        finally:
            session.close()

    @router.post("/api/tasks", status_code=status.HTTP_201_CREATED)
    def create_task(payload: TaskCreateIn) -> dict[str, object]:
        session = session_factory()
        try:
            now = utcnow()
            task = CrawlTask(
                city=payload.city.strip(),
                store=payload.store.strip(),
                frequency_hours=payload.frequency_hours,
                parser_name=payload.parser_name.strip(),
                is_active=payload.is_active,
                created_at=now,
                updated_at=now,
            )
            session.add(task)
            session.commit()
            session.refresh(task)
            LOGGER.info(
                "Dashboard task created: id=%s city=%s store=%s parser=%s active=%s",
                task.id,
                task.city,
                task.store,
                task.parser_name,
                task.is_active,
            )
            return task_to_dict(task, now=now)
        finally:
            session.close()

    @router.patch("/api/tasks/{task_id}")
    def update_task(task_id: int, payload: TaskUpdateIn) -> dict[str, object]:
        session = session_factory()
        try:
            task = session.get(CrawlTask, task_id)
            if task is None or task.deleted_at is not None:
                LOGGER.warning("Dashboard task update failed: task_id=%s reason=not_found", task_id)
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
                "Dashboard task updated: id=%s active=%s frequency_hours=%s parser=%s",
                task.id,
                task.is_active,
                task.frequency_hours,
                task.parser_name,
            )
            return task_to_dict(task, now=utcnow())
        finally:
            session.close()

    @router.delete("/api/tasks/{task_id}")
    def delete_task(task_id: int) -> dict[str, object]:
        session = session_factory()
        try:
            task = session.get(CrawlTask, task_id)
            if task is None or task.deleted_at is not None:
                LOGGER.warning("Dashboard task delete failed: task_id=%s reason=not_found", task_id)
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")

            now = utcnow()
            task.deleted_at = now
            task.is_active = False
            task.lease_owner_id = None
            task.lease_until = None
            task.updated_at = now
            session.commit()
            LOGGER.info("Dashboard task deleted: id=%s", task_id)
            return {"ok": True, "task_id": task_id}
        finally:
            session.close()

    @router.websocket("/ws/runs/{run_id}/log")
    async def stream_run_log(
        websocket: WebSocket,
        run_id: str,
        tail: int = Query(default=200, ge=0, le=5000),
    ) -> None:
        await websocket.accept()
        parser_socket: Any | None = None

        async def _send_error(message: str) -> None:
            with contextlib.suppress(Exception):
                await websocket.send_json(
                    {
                        "ok": False,
                        "action": "stream_job_log",
                        "event": "error",
                        "run_id": run_id,
                        "error": message,
                    }
                )

        try:
            # Keep DB checkout short: fetch run metadata once and release connection
            # before opening long-lived websocket stream to orchestrator.
            session = session_factory()
            try:
                run = session.get(TaskRun, run_id)
                if run is None:
                    await _send_error("Run not found.")
                    LOGGER.warning("Run log stream failed: run_id=%s reason=run_not_found", run_id)
                    return
                run_dispatch_meta = dispatch_meta(run.dispatch_meta_json)
            finally:
                session.close()

            remote_job_id = run_dispatch_meta.get("remote_job_id")
            if not isinstance(remote_job_id, str) or not remote_job_id.strip():
                await _send_error("Run is not linked to orchestrator WS job.")
                LOGGER.warning("Run log stream failed: run_id=%s reason=missing_remote_job_id", run_id)
                return

            connector = connect_orchestrator_ws_getter()
            parser_socket = await connector(app_settings.orchestrator_ws_url)
            request_payload: dict[str, Any] = {
                "action": "stream_job_log",
                "job_id": remote_job_id.strip(),
                "tail_lines": int(tail),
            }
            if app_settings.orchestrator_ws_password is not None:
                request_payload["password"] = app_settings.orchestrator_ws_password
            await parser_socket.send(json.dumps(request_payload, ensure_ascii=False))
            LOGGER.info(
                "Run log stream started: run_id=%s remote_job_id=%s tail=%s",
                run_id,
                remote_job_id.strip(),
                tail,
            )

            while True:
                raw_payload = await parser_socket.recv()
                raw_text = (
                    raw_payload.decode("utf-8", errors="replace")
                    if isinstance(raw_payload, (bytes, bytearray))
                    else str(raw_payload)
                )
                try:
                    parsed_payload = json.loads(raw_text)
                except json.JSONDecodeError:
                    await _send_error("Orchestrator returned invalid JSON.")
                    return

                if not isinstance(parsed_payload, dict):
                    await _send_error("Orchestrator returned unexpected payload format.")
                    return

                await websocket.send_json(parsed_payload)
                event_name = str(parsed_payload.get("event", "")).strip().lower()
                if parsed_payload.get("ok") is False or event_name in {"end", "error"}:
                    return
        except WebSocketDisconnect:
            LOGGER.debug("Dashboard client disconnected from run-log stream: run_id=%s", run_id)
        except Exception as exc:
            LOGGER.exception("Run log proxy failed: run_id=%s error=%s", run_id, exc)
            await _send_error(str(exc))
        finally:
            if parser_socket is not None:
                with contextlib.suppress(Exception):
                    await parser_socket.close()
            with contextlib.suppress(Exception):
                await websocket.close()

    @router.get("/api/overview")
    def overview() -> dict[str, object]:
        session = session_factory()
        try:
            now = utcnow()
            tasks = session.scalars(select(CrawlTask).where(CrawlTask.deleted_at.is_(None))).all()
            orchestrators = session.scalars(
                select(Orchestrator).order_by(Orchestrator.updated_at.desc())
            ).all()

            due_count = sum(1 for task in tasks if task.is_active and is_task_due(task, now=now))
            leased_count = sum(
                1
                for task in tasks
                if (
                    task.lease_owner_id is not None
                    and (lease_until := as_utc(task.lease_until)) is not None
                    and lease_until > now
                )
            )

            run_counts = dict(
                session.execute(select(TaskRun.status, func.count(TaskRun.id)).group_by(TaskRun.status)).all()
            )
            warning_runs = int(
                session.scalar(
                    select(func.count(TaskRun.id))
                    .join(RunArtifact, RunArtifact.run_id == TaskRun.id)
                    .where(
                        TaskRun.status == "success",
                        RunArtifact.dataclass_validated.is_(False),
                    )
                )
                or 0
            )

            recent_rows = session.execute(
                select(
                    TaskRun.id,
                    TaskRun.task_id,
                    TaskRun.status,
                    TaskRun.assigned_at,
                    TaskRun.finished_at,
                    Orchestrator.name,
                    CrawlTask.city,
                    CrawlTask.store,
                    TaskRun.processed_images,
                    TaskRun.dispatch_meta_json,
                    RunArtifact.dataclass_validated,
                )
                .join(Orchestrator, Orchestrator.id == TaskRun.orchestrator_id)
                .join(CrawlTask, CrawlTask.id == TaskRun.task_id)
                .outerjoin(RunArtifact, RunArtifact.run_id == TaskRun.id)
                .order_by(TaskRun.assigned_at.desc())
                .limit(12)
            ).all()

            recent_runs = []
            for (
                run_id,
                task_id,
                run_status,
                assigned_at,
                finished_at,
                orchestrator_name,
                city,
                store,
                processed_images,
                run_dispatch_meta,
                dataclass_validated,
            ) in recent_rows:
                run_meta = dispatch_meta(run_dispatch_meta)
                remote_status = str(run_meta.get("remote_status", "")).strip().lower() or None
                remote_terminal = remote_status in {"success", "error"}
                remote_job_id = run_meta.get("remote_job_id")
                has_remote_job_id = isinstance(remote_job_id, str) and bool(remote_job_id.strip())
                local_terminal = str(run_status).strip().lower() in {"success", "error"}
                validation_failed = bool(run_status == "success" and dataclass_validated is False)
                display_status = "validation_failed" if validation_failed else str(run_status)
                recent_runs.append(
                    {
                        "id": run_id,
                        "task_id": task_id,
                        "status": run_status,
                        "display_status": display_status,
                        "assigned_at": as_utc(assigned_at).isoformat(),
                        "finished_at": as_utc(finished_at).isoformat() if finished_at else None,
                        "orchestrator_name": orchestrator_name,
                        "city": city,
                        "store": store,
                        "processed_images": int(processed_images or 0),
                        "remote_status": remote_status,
                        "remote_terminal": bool(remote_terminal),
                        "validation_failed": validation_failed,
                        "dataclass_validated": dataclass_validated,
                        "can_open_live_log": bool(not local_terminal and not remote_terminal and has_remote_job_id),
                    }
                )

            response_payload = {
                "generated_at": now.isoformat(),
                "tasks_total": len(tasks),
                "tasks_active": sum(1 for task in tasks if task.is_active),
                "tasks_due": due_count,
                "tasks_leased": leased_count,
                "orchestrators_total": len(orchestrators),
                "runs_total": sum(run_counts.values()),
                "runs_assigned": int(run_counts.get("assigned", 0)),
                "runs_success": int(run_counts.get("success", 0)),
                "runs_error": int(run_counts.get("error", 0)),
                "runs_warning": warning_runs,
                "orchestrators": [
                    {
                        "id": item.id,
                        "name": item.name,
                        "created_at": as_utc(item.created_at).isoformat(),
                        "updated_at": as_utc(item.updated_at).isoformat(),
                        "last_heartbeat_at": as_utc(item.last_heartbeat_at).isoformat(),
                    }
                    for item in orchestrators
                ],
                "recent_runs": recent_runs,
            }
            LOGGER.debug("Dashboard overview generated: tasks=%s orchestrators=%s", len(tasks), len(orchestrators))
            return response_payload
        finally:
            session.close()

    return router
