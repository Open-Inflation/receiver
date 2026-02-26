from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from ..models import CrawlTask, Orchestrator, TaskRun
from .scheduler import (
    TERMINAL_RUN_STATUSES,
    claim_next_due_task,
    create_or_get_orchestrator,
    finish_run,
    touch_orchestrator,
    utcnow,
)


LOGGER = logging.getLogger(__name__)


class ParserWsBridge:
    def __init__(
        self,
        *,
        session_factory: sessionmaker[Session],
        parser_bridge: Any,
        image_pipeline: Any,
        artifact_ingestor: Any,
        lease_ttl_minutes: int,
        ws_url: str,
        ws_password: str | None,
        poll_interval_sec: float,
        manager_name: str,
        submit_include_images: bool,
        submit_full_catalog: bool,
        upload_archive_images: bool,
    ):
        self._session_factory = session_factory
        self._parser_bridge = parser_bridge
        self._image_pipeline = image_pipeline
        self._artifact_ingestor = artifact_ingestor
        self._lease_ttl_minutes = max(1, int(lease_ttl_minutes))
        self._ws_url = ws_url
        self._ws_password = ws_password
        self._poll_interval_sec = max(0.5, float(poll_interval_sec))
        self._manager_name = manager_name
        self._submit_include_images = submit_include_images
        self._submit_full_catalog = submit_full_catalog
        self._upload_archive_images = upload_archive_images

        self._stop_event = asyncio.Event()
        self._manager_orchestrator_id: str | None = None
        self._ws_module: Any | None = None
        self._ws_socket: Any | None = None
        self._ws_lock = asyncio.Lock()

    async def run_forever(self) -> None:
        LOGGER.info(
            "Parser WS bridge started: ws_url=%s manager=%s poll_interval=%.1fs",
            self._ws_url,
            self._manager_name,
            self._poll_interval_sec,
        )
        while not self._stop_event.is_set():
            try:
                await self.run_cycle()
            except Exception:
                LOGGER.exception("Parser WS bridge cycle failed")

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._poll_interval_sec)
            except asyncio.TimeoutError:
                continue

        async with self._ws_lock:
            await self._close_ws_connection()
        LOGGER.info("Parser WS bridge stopped")

    async def stop(self) -> None:
        self._stop_event.set()
        async with self._ws_lock:
            await self._close_ws_connection()

    async def run_cycle(self) -> None:
        ping = await self._ws_request({"action": "ping"})
        if not ping.get("ok"):
            LOGGER.debug("Parser WS ping failed: %s", ping)
            return

        session = self._session_factory()
        try:
            orchestrator = self._resolve_manager_orchestrator(session)
            await self._process_assigned_runs(session, orchestrator)
            await self._claim_and_submit_due_tasks(session, orchestrator)
            LOGGER.debug("Parser WS cycle completed: orchestrator_id=%s", orchestrator.id)
        finally:
            session.close()

    def _resolve_manager_orchestrator(self, session: Session) -> Orchestrator:
        if self._manager_orchestrator_id:
            orchestrator = session.get(Orchestrator, self._manager_orchestrator_id)
            if orchestrator is not None:
                touch_orchestrator(session, orchestrator, commit=True)
                return orchestrator

        orchestrator = create_or_get_orchestrator(session, name=self._manager_name)
        self._manager_orchestrator_id = orchestrator.id
        return orchestrator

    async def _claim_and_submit_due_tasks(self, session: Session, orchestrator: Orchestrator) -> None:
        submitted_count = 0
        while True:
            claimed = claim_next_due_task(
                session,
                orchestrator=orchestrator,
                lease_ttl_minutes=self._lease_ttl_minutes,
            )
            if claimed is None:
                break

            task, run = claimed
            await self._submit_run(session, run=run, task=task)
            submitted_count += 1
        if submitted_count:
            LOGGER.info(
                "Submitted claimed tasks: orchestrator_id=%s count=%s",
                orchestrator.id,
                submitted_count,
            )

    async def _process_assigned_runs(self, session: Session, orchestrator: Orchestrator) -> None:
        assigned_runs = session.scalars(
            select(TaskRun)
            .where(
                TaskRun.status == "assigned",
                TaskRun.orchestrator_id == orchestrator.id,
            )
            .order_by(TaskRun.assigned_at.asc())
        ).all()

        for run in assigned_runs:
            if run.status in TERMINAL_RUN_STATUSES:
                continue

            dispatch_meta = self._dispatch_meta(run.dispatch_meta_json)
            remote_job_id = dispatch_meta.get("remote_job_id")
            if not isinstance(remote_job_id, str) or not remote_job_id.strip():
                task = session.get(CrawlTask, run.task_id)
                if task is None:
                    continue
                await self._submit_run(session, run=run, task=task)
                continue

            status_response = await self._ws_request(
                {
                    "action": "status",
                    "job_id": remote_job_id,
                }
            )
            if not status_response.get("ok"):
                error_text = str(status_response.get("error", "status request failed"))
                self._set_dispatch_meta(
                    session,
                    run,
                    {
                        "last_status_error": error_text,
                        "last_status_error_at": utcnow().isoformat(),
                    },
                )
                if "job not found" in error_text.lower():
                    finish_run(
                        session,
                        run=run,
                        orchestrator=orchestrator,
                        status="error",
                        processed_images=0,
                        error_message=f"Remote job not found: {remote_job_id}",
                    )
                continue

            job_payload = status_response.get("job")
            if not isinstance(job_payload, dict):
                continue

            remote_status = str(job_payload.get("status", "")).lower().strip()
            self._set_dispatch_meta(
                session,
                run,
                {
                    "remote_status": remote_status,
                    "last_poll_at": utcnow().isoformat(),
                },
            )

            if remote_status not in {"success", "error"}:
                continue

            image_results: list[dict[str, Any]] = []
            output_gz = self._safe_str(job_payload.get("output_gz"))
            if remote_status == "success" and self._upload_archive_images:
                image_results = await self._process_archive_images(output_gz)

            error_message = self._safe_str(job_payload.get("message"))
            if remote_status == "error" and not error_message:
                error_message = self._safe_str(job_payload.get("traceback"))
            if remote_status == "error" and not error_message:
                error_message = "Orchestrator returned error status"

            processed_images = sum(
                1 for item in image_results if isinstance(item, dict) and item.get("uploaded_url")
            )
            finished_run = finish_run(
                session,
                run=run,
                orchestrator=orchestrator,
                status=remote_status,
                processed_images=processed_images,
                error_message=error_message,
            )
            if remote_status == "success":
                try:
                    ingest_result = self._artifact_ingestor.ingest_run_output(
                        session,
                        run=finished_run,
                        output_json=self._safe_str(job_payload.get("output_json")),
                        output_gz=output_gz,
                        download_url=self._safe_str(job_payload.get("download_url")),
                        download_sha256=self._safe_str(job_payload.get("download_sha256")),
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

    async def _submit_run(self, session: Session, *, run: TaskRun, task: CrawlTask) -> None:
        payload: dict[str, Any] = {
            "action": "submit_store",
            "store_code": task.store,
            "parser": task.parser_name,
            "include_images": self._submit_include_images,
            "full_catalog": self._submit_full_catalog,
        }
        city_token = task.city.strip()
        if city_token:
            try:
                payload["city_id"] = int(city_token)
            except ValueError:
                payload["city_id"] = city_token

        response = await self._ws_request(payload)
        sanitized_request = dict(payload)

        if response.get("ok") and isinstance(response.get("job_id"), str):
            self._set_dispatch_meta(
                session,
                run,
                {
                    "submitted_at": utcnow().isoformat(),
                    "remote_job_id": response.get("job_id"),
                    "remote_status": response.get("status"),
                    "request": sanitized_request,
                    "response": response,
                },
            )
            LOGGER.info(
                "Submitted task %s store=%s parser=%s remote_job_id=%s",
                run.task_id,
                task.store,
                task.parser_name,
                response.get("job_id"),
            )
            return

        self._set_dispatch_meta(
            session,
            run,
            {
                "submit_error": str(response.get("error", "submit_store failed")),
                "last_submit_attempt_at": utcnow().isoformat(),
                "request": sanitized_request,
                "response": response,
            },
        )
        LOGGER.warning(
            "Failed to submit task %s store=%s: %s",
            run.task_id,
            task.store,
            response,
        )

    def _set_dispatch_meta(self, session: Session, run: TaskRun, patch: dict[str, Any]) -> None:
        meta: dict[str, Any] = dict(run.dispatch_meta_json) if isinstance(run.dispatch_meta_json, dict) else {}
        meta.update(patch)
        run.dispatch_meta_json = meta
        session.commit()
        session.refresh(run)

    @staticmethod
    def _dispatch_meta(value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            return {}
        return value

    @staticmethod
    def _safe_str(value: Any) -> str | None:
        if value is None:
            return None
        token = str(value).strip()
        return token or None

    async def _process_archive_images(self, archive_path: str | None) -> list[dict[str, Any]]:
        async_method = getattr(self._image_pipeline, "process_archive_images_async", None)
        if callable(async_method):
            result = async_method(archive_path)
            if asyncio.iscoroutine(result):
                LOGGER.debug("Using async archive image pipeline")
                return await result
            if isinstance(result, list):
                LOGGER.debug("Async archive image pipeline returned immediate list result")
                return result

        sync_method = getattr(self._image_pipeline, "process_archive_images", None)
        if callable(sync_method):
            LOGGER.debug("Using sync archive image pipeline fallback")
            result = sync_method(archive_path)
            if isinstance(result, list):
                return result
        LOGGER.warning("Image pipeline does not provide archive image processing")
        return []

    async def _ws_request(self, payload: dict[str, Any]) -> dict[str, Any]:
        request_payload = dict(payload)
        if self._ws_password is not None:
            request_payload["password"] = self._ws_password

        async with self._ws_lock:
            for attempt in range(2):
                try:
                    socket = await self._ensure_ws_connection()
                except RuntimeError as exc:
                    return {"ok": False, "error": str(exc)}

                try:
                    await socket.send(json.dumps(request_payload, ensure_ascii=False))
                    raw = await socket.recv()
                except Exception as exc:
                    await self._close_ws_connection()
                    if attempt == 0:
                        continue
                    return {"ok": False, "error": f"WS request failed: {exc}"}

                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    return {"ok": False, "error": "Orchestrator returned invalid JSON"}

                if not isinstance(data, dict):
                    return {"ok": False, "error": "Orchestrator response is not a JSON object"}
                return data

        return {"ok": False, "error": "WS request failed unexpectedly"}

    async def _ensure_ws_connection(self) -> Any:
        if self._ws_socket is not None and self._socket_is_open(self._ws_socket):
            return self._ws_socket

        websockets = self._load_websockets_module()
        if websockets is None:
            raise RuntimeError("websockets package is not installed")

        try:
            self._ws_socket = await websockets.connect(self._ws_url)
            LOGGER.debug("Parser WS connected: %s", self._ws_url)
        except Exception as exc:
            self._ws_socket = None
            LOGGER.debug("Parser WS connection failed: %s", exc)
            raise RuntimeError(f"WS connect failed: {exc}") from exc
        return self._ws_socket

    def _load_websockets_module(self) -> Any | None:
        if self._ws_module is not None:
            return self._ws_module

        try:
            import websockets
        except ModuleNotFoundError:
            LOGGER.error("Package 'websockets' is required for orchestrator bridge")
            return None

        self._ws_module = websockets
        return websockets

    async def _close_ws_connection(self) -> None:
        socket = self._ws_socket
        self._ws_socket = None
        if socket is None:
            return
        try:
            await socket.close()
        except Exception:
            LOGGER.debug("Parser WS close failed", exc_info=True)

    @staticmethod
    def _socket_is_open(socket: Any) -> bool:
        state = getattr(socket, "state", None)
        if state is not None:
            state_text = str(state).lower()
            if "closed" in state_text or "closing" in state_text:
                return False
            return True

        closed = getattr(socket, "closed", None)
        if isinstance(closed, bool):
            return not closed
        return True
