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
        ws_request_timeout_sec: float = 15.0,
        max_claims_per_cycle: int = 5,
        assigned_parallelism: int = 4,
    ):
        self._session_factory = session_factory
        self._parser_bridge = parser_bridge
        self._image_pipeline = image_pipeline
        self._artifact_ingestor = artifact_ingestor
        self._lease_ttl_minutes = max(1, int(lease_ttl_minutes))
        self._ws_url = ws_url
        self._ws_password = ws_password
        self._poll_interval_sec = max(0.5, float(poll_interval_sec))
        self._ws_request_timeout_sec = max(1.0, float(ws_request_timeout_sec))
        self._max_claims_per_cycle = max(1, int(max_claims_per_cycle))
        self._assigned_parallelism = max(1, int(assigned_parallelism))
        self._manager_name = manager_name
        self._default_submit_include_images = submit_include_images
        self._submit_full_catalog = submit_full_catalog
        self._upload_archive_images = upload_archive_images

        self._stop_event = asyncio.Event()
        self._manager_orchestrator_id: str | None = None
        self._ws_module: Any | None = None
        self._ws_socket: Any | None = None
        self._ws_lock = asyncio.Lock()

    async def run_forever(self) -> None:
        LOGGER.info(
            "Parser WS bridge started: ws_url=%s manager=%s poll_interval=%.1fs request_timeout=%.1fs max_claims_per_cycle=%s assigned_parallelism=%s",
            self._ws_url,
            self._manager_name,
            self._poll_interval_sec,
            self._ws_request_timeout_sec,
            self._max_claims_per_cycle,
            self._assigned_parallelism,
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
            orchestrator_id = orchestrator.id
        finally:
            session.close()

        await self._process_assigned_runs(orchestrator_id=orchestrator_id)

        claim_session = self._session_factory()
        try:
            orchestrator = claim_session.get(Orchestrator, orchestrator_id)
            if orchestrator is None:
                orchestrator = self._resolve_manager_orchestrator(claim_session)
            await self._claim_and_submit_due_tasks(claim_session, orchestrator)
            LOGGER.debug("Parser WS cycle completed: orchestrator_id=%s", orchestrator.id)
        finally:
            claim_session.close()

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
        while submitted_count < self._max_claims_per_cycle:
            claimed = claim_next_due_task(
                session,
                orchestrator=orchestrator,
                lease_ttl_minutes=self._lease_ttl_minutes,
            )
            if claimed is None:
                break

            task, run = claimed
            await self._submit_run(session, run=run, task=task, orchestrator=orchestrator)
            submitted_count += 1
        if submitted_count >= self._max_claims_per_cycle:
            LOGGER.info(
                "Claim cycle limit reached: orchestrator_id=%s submitted=%s limit=%s",
                orchestrator.id,
                submitted_count,
                self._max_claims_per_cycle,
            )
        LOGGER.info(
            "Claim cycle finished: orchestrator_id=%s submitted=%s limit=%s",
            orchestrator.id,
            submitted_count,
            self._max_claims_per_cycle,
        )

    async def _process_assigned_runs(self, *, orchestrator_id: str) -> None:
        session = self._session_factory()
        try:
            assigned_run_ids = session.scalars(
                select(TaskRun.id)
                .where(
                    TaskRun.status == "assigned",
                    TaskRun.orchestrator_id == orchestrator_id,
                )
                .order_by(TaskRun.assigned_at.asc())
            ).all()
        finally:
            session.close()

        LOGGER.debug(
            "Assigned runs fetched: orchestrator_id=%s count=%s parallelism=%s",
            orchestrator_id,
            len(assigned_run_ids),
            self._assigned_parallelism,
        )
        if not assigned_run_ids:
            return

        semaphore = asyncio.Semaphore(self._assigned_parallelism)

        async def _worker(run_id: str) -> None:
            async with semaphore:
                await self._process_single_assigned_run(orchestrator_id=orchestrator_id, run_id=run_id)

        results = await asyncio.gather(*[_worker(run_id) for run_id in assigned_run_ids], return_exceptions=True)
        failed = sum(1 for item in results if isinstance(item, Exception))
        if failed:
            LOGGER.warning(
                "Assigned run processing completed with failures: orchestrator_id=%s total=%s failed=%s",
                orchestrator_id,
                len(assigned_run_ids),
                failed,
            )
        else:
            LOGGER.debug(
                "Assigned run processing completed: orchestrator_id=%s total=%s",
                orchestrator_id,
                len(assigned_run_ids),
            )


    async def _process_single_assigned_run(self, *, orchestrator_id: str, run_id: str) -> None:
        session = self._session_factory()
        run: TaskRun | None = None
        orchestrator: Orchestrator | None = None
        try:
            run = session.get(TaskRun, run_id)
            if run is None:
                LOGGER.debug("Assigned run not found while processing: run_id=%s", run_id)
                return
            if run.status != "assigned":
                LOGGER.debug("Skipping non-assigned run: run_id=%s status=%s", run.id, run.status)
                return
            if run.orchestrator_id != orchestrator_id:
                LOGGER.debug(
                    "Skipping run with changed owner: run_id=%s expected_orchestrator=%s actual_orchestrator=%s",
                    run.id,
                    orchestrator_id,
                    run.orchestrator_id,
                )
                return

            orchestrator = session.get(Orchestrator, orchestrator_id)
            if orchestrator is None:
                LOGGER.warning("Run processing skipped, orchestrator not found: run_id=%s", run.id)
                return

            dispatch_meta = self._dispatch_meta(run.dispatch_meta_json)
            remote_job_id = self._safe_str(dispatch_meta.get("remote_job_id"))
            persisted_remote_status = str(dispatch_meta.get("remote_status", "")).strip().lower()
            if persisted_remote_status == "error":
                self._mark_run_error(
                    session,
                    run=run,
                    orchestrator=orchestrator,
                    error_message=self._safe_str(dispatch_meta.get("last_status_error"))
                    or "Remote job marked as error",
                    meta_patch={
                        "last_finalize_error": "Reconciled from persisted remote_status=error",
                        "last_finalize_error_at": utcnow().isoformat(),
                    },
                    processed_images=int(run.processed_images or 0),
                )
                LOGGER.warning(
                    "Run reconciled from persisted remote_status=error: run_id=%s remote_job_id=%s",
                    run.id,
                    remote_job_id,
                )
                return

            if not remote_job_id:
                LOGGER.debug("Run has no remote_job_id, resubmitting: run_id=%s task_id=%s", run.id, run.task_id)
                task = session.get(CrawlTask, run.task_id)
                if task is None:
                    self._mark_run_error(
                        session,
                        run=run,
                        orchestrator=orchestrator,
                        error_message=f"Task not found for assigned run: {run.task_id}",
                        meta_patch={
                            "last_finalize_error": "Task missing during resubmit",
                            "last_finalize_error_at": utcnow().isoformat(),
                        },
                        processed_images=int(run.processed_images or 0),
                    )
                    return
                await self._submit_run(session, run=run, task=task, orchestrator=orchestrator)
                return

            status_response = await self._ws_request(
                {
                    "action": "status",
                    "job_id": remote_job_id,
                }
            )
            if not status_response.get("ok"):
                error_text = str(status_response.get("error", "status request failed"))
                LOGGER.warning(
                    "Status request failed: run_id=%s remote_job_id=%s error=%s",
                    run.id,
                    remote_job_id,
                    error_text,
                )
                self._mark_run_error(
                    session,
                    run=run,
                    orchestrator=orchestrator,
                    error_message=f"Status request failed: {error_text}",
                    meta_patch={
                        "last_status_error": error_text,
                        "last_status_error_at": utcnow().isoformat(),
                    },
                    processed_images=int(run.processed_images or 0),
                )
                return

            job_payload = status_response.get("job")
            if not isinstance(job_payload, dict):
                LOGGER.warning(
                    "Status response without job payload: run_id=%s remote_job_id=%s payload=%s",
                    run.id,
                    remote_job_id,
                    status_response,
                )
                self._mark_run_error(
                    session,
                    run=run,
                    orchestrator=orchestrator,
                    error_message="Orchestrator status payload missing job object",
                    meta_patch={
                        "last_status_error": "status response without job payload",
                        "last_status_error_at": utcnow().isoformat(),
                    },
                    processed_images=int(run.processed_images or 0),
                )
                return

            remote_status = str(job_payload.get("status", "")).lower().strip()
            LOGGER.debug(
                "Remote job status: run_id=%s remote_job_id=%s status=%s",
                run.id,
                remote_job_id,
                remote_status,
            )
            self._set_dispatch_meta(
                session,
                run,
                {
                    "remote_status": remote_status,
                    "last_poll_at": utcnow().isoformat(),
                },
            )

            if remote_status not in {"success", "error"}:
                LOGGER.debug(
                    "Run still in progress: run_id=%s remote_job_id=%s status=%s",
                    run.id,
                    remote_job_id,
                    remote_status,
                )
                return

            processed_images = int(run.processed_images or 0)
            try:
                image_results: list[dict[str, Any]] = []
                output_gz = self._safe_str(job_payload.get("output_gz"))
                if remote_status == "success" and self._upload_archive_images:
                    image_results = await self._process_archive_images(output_gz)
                processed_images = sum(
                    1 for item in image_results if isinstance(item, dict) and item.get("uploaded_url")
                )

                error_message = self._safe_str(job_payload.get("message"))
                if remote_status == "error" and not error_message:
                    error_message = self._safe_str(job_payload.get("traceback"))
                if remote_status == "error" and not error_message:
                    error_message = "Orchestrator returned error status"

                if remote_status == "success":
                    ingest_result = self._artifact_ingestor.ingest_run_output(
                        session,
                        run=run,
                        output_json=self._safe_str(job_payload.get("output_json")),
                        output_gz=output_gz,
                        download_url=self._safe_str(job_payload.get("download_url")),
                        download_sha256=self._safe_str(job_payload.get("download_sha256")),
                        image_results=image_results,
                    )
                    if not ingest_result.get("ok"):
                        raise RuntimeError(str(ingest_result.get("error", "artifact ingest failed")))

                finish_run(
                    session,
                    run=run,
                    orchestrator=orchestrator,
                    status=remote_status,
                    processed_images=processed_images,
                    error_message=error_message,
                )
            except Exception as exc:
                LOGGER.exception(
                    "Failed to finalize run from terminal remote status: run_id=%s remote_job_id=%s remote_status=%s",
                    run.id,
                    remote_job_id,
                    remote_status,
                )
                self._mark_run_error(
                    session,
                    run=run,
                    orchestrator=orchestrator,
                    error_message=f"Finalize failed: {exc}",
                    meta_patch={
                        "last_finalize_error": str(exc),
                        "last_finalize_error_at": utcnow().isoformat(),
                    },
                    processed_images=processed_images,
                )
        except Exception as exc:
            LOGGER.exception("Assigned run processing crashed: run_id=%s", run_id)
            if run is not None and orchestrator is not None and run.status == "assigned":
                self._mark_run_error(
                    session,
                    run=run,
                    orchestrator=orchestrator,
                    error_message=f"Assigned run processing crashed: {exc}",
                    meta_patch={
                        "last_finalize_error": str(exc),
                        "last_finalize_error_at": utcnow().isoformat(),
                    },
                    processed_images=int(run.processed_images or 0),
                )
        finally:
            session.close()

    async def _submit_run(
        self,
        session: Session,
        *,
        run: TaskRun,
        task: CrawlTask,
        orchestrator: Orchestrator,
    ) -> None:
        payload: dict[str, Any] = {
            "action": "submit_store",
            "store_code": task.store,
            "parser": task.parser_name,
            "include_images": self._resolve_task_include_images(task),
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
                "Submitted task %s store=%s parser=%s include_images=%s remote_job_id=%s",
                run.task_id,
                task.store,
                task.parser_name,
                payload["include_images"],
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
                "last_finalize_error": str(response.get("error", "submit_store failed")),
                "last_finalize_error_at": utcnow().isoformat(),
            },
            commit=False,
        )
        self._mark_run_error(
            session,
            run=run,
            orchestrator=orchestrator,
            error_message=f"Submit failed: {response.get('error', 'submit_store failed')}",
            processed_images=int(run.processed_images or 0),
        )
        LOGGER.warning(
            "Failed to submit task %s store=%s: %s",
            run.task_id,
            task.store,
            response,
        )

    def _set_dispatch_meta(
        self,
        session: Session,
        run: TaskRun,
        patch: dict[str, Any],
        *,
        commit: bool = True,
    ) -> None:
        meta: dict[str, Any] = dict(run.dispatch_meta_json) if isinstance(run.dispatch_meta_json, dict) else {}
        meta.update(patch)
        run.dispatch_meta_json = meta
        if commit:
            session.commit()
            session.refresh(run)

    def _mark_run_error(
        self,
        session: Session,
        *,
        run: TaskRun,
        orchestrator: Orchestrator,
        error_message: str,
        meta_patch: dict[str, Any] | None = None,
        processed_images: int | None = None,
    ) -> None:
        if run.status in TERMINAL_RUN_STATUSES:
            LOGGER.debug("Run already terminal, error mark skipped: run_id=%s status=%s", run.id, run.status)
            return

        patch: dict[str, Any] = {"failed_at": utcnow().isoformat()}
        if isinstance(meta_patch, dict):
            patch.update(meta_patch)
        self._set_dispatch_meta(session, run, patch, commit=False)
        safe_processed_images = int(run.processed_images or 0) if processed_images is None else int(processed_images)
        finish_run(
            session,
            run=run,
            orchestrator=orchestrator,
            status="error",
            processed_images=max(0, safe_processed_images),
            error_message=error_message,
        )

    @staticmethod
    def _dispatch_meta(value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            return {}
        return value

    def _resolve_task_include_images(self, task: CrawlTask) -> bool:
        include_images = getattr(task, "include_images", None)
        if isinstance(include_images, bool):
            return include_images
        if include_images is not None:
            return bool(include_images)
        return bool(self._default_submit_include_images)

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
                    await asyncio.wait_for(
                        socket.send(json.dumps(request_payload, ensure_ascii=False)),
                        timeout=self._ws_request_timeout_sec,
                    )
                    raw = await asyncio.wait_for(
                        socket.recv(),
                        timeout=self._ws_request_timeout_sec,
                    )
                except asyncio.TimeoutError:
                    await self._close_ws_connection()
                    if attempt == 0:
                        continue
                    return {
                        "ok": False,
                        "error": (
                            f"WS request timed out after {self._ws_request_timeout_sec:.1f}s "
                            f"(action={request_payload.get('action')})"
                        ),
                    }
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
