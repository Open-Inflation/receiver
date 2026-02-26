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
from app.models import CrawlTask, Orchestrator, TaskRun
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
            tasks = session.scalars(
                select(CrawlTask).where(CrawlTask.deleted_at.is_(None)).order_by(CrawlTask.id.asc())
            ).all()
            return [task_to_dict(task) for task in tasks]
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
            return task_to_dict(task)
        finally:
            session.close()

    @router.patch("/api/tasks/{task_id}")
    def update_task(task_id: int, payload: TaskUpdateIn) -> dict[str, object]:
        session = session_factory()
        try:
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
            return task_to_dict(task)
        finally:
            session.close()

    @router.delete("/api/tasks/{task_id}")
    def delete_task(task_id: int) -> dict[str, object]:
        session = session_factory()
        try:
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
        finally:
            session.close()

    @router.websocket("/ws/runs/{run_id}/log")
    async def stream_run_log(
        websocket: WebSocket,
        run_id: str,
        tail: int = Query(default=200, ge=0, le=5000),
    ) -> None:
        await websocket.accept()
        session = session_factory()
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
            run = session.get(TaskRun, run_id)
            if run is None:
                await _send_error("Run not found.")
                return

            run_dispatch_meta = dispatch_meta(run.payload_json)
            remote_job_id = run_dispatch_meta.get("remote_job_id")
            if not isinstance(remote_job_id, str) or not remote_job_id.strip():
                await _send_error("Run is not linked to orchestrator WS job.")
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
            session.close()
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

            recent_runs_limited = (
                select(
                    TaskRun.id.label("run_id"),
                    TaskRun.task_id.label("task_id"),
                    TaskRun.orchestrator_id.label("orchestrator_id"),
                    TaskRun.status.label("status"),
                    TaskRun.assigned_at.label("assigned_at"),
                    TaskRun.finished_at.label("finished_at"),
                )
                .order_by(TaskRun.assigned_at.desc())
                .limit(12)
                .subquery()
            )
            recent_rows = session.execute(
                select(
                    recent_runs_limited.c.run_id,
                    recent_runs_limited.c.task_id,
                    recent_runs_limited.c.status,
                    recent_runs_limited.c.assigned_at,
                    recent_runs_limited.c.finished_at,
                    Orchestrator.name,
                    CrawlTask.city,
                    CrawlTask.store,
                    TaskRun.image_results_json,
                )
                .join(Orchestrator, Orchestrator.id == recent_runs_limited.c.orchestrator_id)
                .join(CrawlTask, CrawlTask.id == recent_runs_limited.c.task_id)
                .join(TaskRun, TaskRun.id == recent_runs_limited.c.run_id)
                .order_by(recent_runs_limited.c.assigned_at.desc())
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
                image_results,
            ) in recent_rows:
                processed_images = 0
                if isinstance(image_results, list):
                    processed_images = sum(
                        1 for item in image_results if isinstance(item, dict) and item.get("uploaded_url")
                    )
                recent_runs.append(
                    {
                        "id": run_id,
                        "task_id": task_id,
                        "status": run_status,
                        "assigned_at": assigned_at.isoformat(),
                        "finished_at": finished_at.isoformat() if finished_at else None,
                        "orchestrator_name": orchestrator_name,
                        "city": city,
                        "store": store,
                        "processed_images": processed_images,
                    }
                )

            return {
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
                "orchestrators": [
                    {
                        "id": item.id,
                        "name": item.name,
                        "created_at": item.created_at.isoformat(),
                        "updated_at": item.updated_at.isoformat(),
                        "last_heartbeat_at": item.last_heartbeat_at.isoformat(),
                    }
                    for item in orchestrators
                ],
                "recent_runs": recent_runs,
            }
        finally:
            session.close()

    return router
