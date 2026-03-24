from __future__ import annotations

import contextlib
from datetime import timedelta
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, WebSocket, WebSocketDisconnect, status
from sqlalchemy import func, select, update
from sqlalchemy.orm import Session

from app.config import Settings
from app.models import CrawlTask, Orchestrator, ParserStoreDirectory, TaskRun
from app.services.scheduler import (
    TERMINAL_RUN_STATUSES,
    as_utc,
    create_or_get_orchestrator,
    finish_run,
    is_task_due,
    utcnow,
)

from .schemas import StoreDirectorySyncIn, TaskCreateIn, TaskUpdateIn
from .utils import dispatch_meta, task_to_dict

LOGGER = logging.getLogger(__name__)
SUPPORTED_STORE_DIRECTORY_PARSERS = {"fixprice", "chizhik"}

OrchestratorWsConnector = Callable[[str], Awaitable[Any]]
OrchestratorWsConnectorGetter = Callable[[], OrchestratorWsConnector]
SessionFactory = Callable[[], Session]


def _safe_non_empty_str(value: Any) -> str | None:
    if value is None:
        return None
    token = str(value).strip()
    return token or None


def _safe_float(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        token = value.strip()
        if not token:
            return None
        try:
            return float(token)
        except ValueError:
            return None
    return None


def _safe_schedule_value(value: Any) -> str | None:
    token = _safe_non_empty_str(value)
    if token is None:
        return None
    return token[:16]


def _store_directory_to_dict(row: ParserStoreDirectory) -> dict[str, object]:
    return {
        "id": int(row.id),
        "parser_name": row.parser_name,
        "store_code": row.store_code,
        "city_name": row.city_name,
        "city_alias": row.city_alias,
        "country": row.country,
        "region": row.region,
        "retail_type": row.retail_type,
        "address": row.address,
        "schedule_weekdays_open_from": row.schedule_weekdays_open_from,
        "schedule_weekdays_closed_from": row.schedule_weekdays_closed_from,
        "schedule_saturday_open_from": row.schedule_saturday_open_from,
        "schedule_saturday_closed_from": row.schedule_saturday_closed_from,
        "schedule_sunday_open_from": row.schedule_sunday_open_from,
        "schedule_sunday_closed_from": row.schedule_sunday_closed_from,
        "temporarily_closed": row.temporarily_closed,
        "longitude": row.longitude,
        "latitude": row.latitude,
        "is_partial": bool(row.is_partial),
        "is_active": bool(row.is_active),
        "first_seen_at": as_utc(row.first_seen_at).isoformat(),
        "last_seen_at": as_utc(row.last_seen_at).isoformat(),
        "updated_at": as_utc(row.updated_at).isoformat(),
    }


def _normalize_store_directory_payload(
    *,
    parser_name: str,
    raw_store: dict[str, Any],
) -> tuple[dict[str, Any] | None, str | None]:
    store_code = _safe_non_empty_str(raw_store.get("code"))
    if store_code is None:
        return None, "store entry skipped: missing code"

    administrative_unit = (
        raw_store.get("administrative_unit")
        if isinstance(raw_store.get("administrative_unit"), dict)
        else {}
    )
    schedule_weekdays = (
        raw_store.get("schedule_weekdays")
        if isinstance(raw_store.get("schedule_weekdays"), dict)
        else {}
    )
    schedule_saturday = (
        raw_store.get("schedule_saturday")
        if isinstance(raw_store.get("schedule_saturday"), dict)
        else {}
    )
    schedule_sunday = (
        raw_store.get("schedule_sunday")
        if isinstance(raw_store.get("schedule_sunday"), dict)
        else {}
    )

    longitude = _safe_float(raw_store.get("longitude"))
    if longitude is None:
        longitude = _safe_float(administrative_unit.get("longitude"))
    latitude = _safe_float(raw_store.get("latitude"))
    if latitude is None:
        latitude = _safe_float(administrative_unit.get("latitude"))
    address = _safe_non_empty_str(raw_store.get("address"))
    is_partial = bool(address is None or longitude is None or latitude is None)

    payload = dict(raw_store)
    payload.pop("categories", None)
    payload.pop("products", None)

    normalized = {
        "parser_name": parser_name,
        "store_code": store_code,
        "city_name": _safe_non_empty_str(administrative_unit.get("name")),
        "city_alias": _safe_non_empty_str(administrative_unit.get("alias")),
        "country": _safe_non_empty_str(administrative_unit.get("country")),
        "region": _safe_non_empty_str(administrative_unit.get("region")),
        "retail_type": _safe_non_empty_str(raw_store.get("retail_type")),
        "address": address,
        "schedule_weekdays_open_from": _safe_schedule_value(schedule_weekdays.get("open_from")),
        "schedule_weekdays_closed_from": _safe_schedule_value(schedule_weekdays.get("closed_from")),
        "schedule_saturday_open_from": _safe_schedule_value(schedule_saturday.get("open_from")),
        "schedule_saturday_closed_from": _safe_schedule_value(schedule_saturday.get("closed_from")),
        "schedule_sunday_open_from": _safe_schedule_value(schedule_sunday.get("open_from")),
        "schedule_sunday_closed_from": _safe_schedule_value(schedule_sunday.get("closed_from")),
        "temporarily_closed": (
            raw_store.get("temporarily_closed")
            if isinstance(raw_store.get("temporarily_closed"), bool)
            else None
        ),
        "longitude": longitude,
        "latitude": latitude,
        "payload_json": payload,
        "is_partial": is_partial,
    }
    return normalized, None


def create_dashboard_router(
    *,
    session_factory: SessionFactory,
    app_settings: Settings,
    connect_orchestrator_ws_getter: OrchestratorWsConnectorGetter,
) -> APIRouter:
    router = APIRouter()

    async def _orchestrator_ws_call(payload: dict[str, Any]) -> dict[str, Any]:
        parser_socket: Any | None = None
        try:
            connector = connect_orchestrator_ws_getter()
            parser_socket = await connector(app_settings.orchestrator_ws_url)
            request_payload = dict(payload)
            if app_settings.orchestrator_ws_password is not None:
                request_payload["password"] = app_settings.orchestrator_ws_password
            await parser_socket.send(json.dumps(request_payload, ensure_ascii=False))
            raw_payload = await parser_socket.recv()
            raw_text = (
                raw_payload.decode("utf-8", errors="replace")
                if isinstance(raw_payload, (bytes, bytearray))
                else str(raw_payload)
            )
            parsed_payload = json.loads(raw_text)
            if not isinstance(parsed_payload, dict):
                raise RuntimeError("Orchestrator returned unexpected payload format")
            return parsed_payload
        finally:
            if parser_socket is not None:
                with contextlib.suppress(Exception):
                    await parser_socket.close()

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
            include_images = (
                app_settings.orchestrator_submit_include_images
                if payload.include_images is None
                else payload.include_images
            )
            use_product_info = (
                app_settings.orchestrator_submit_use_product_info
                if payload.use_product_info is None
                else payload.use_product_info
            )
            task = CrawlTask(
                city="",
                store=payload.store.strip(),
                frequency_hours=payload.frequency_hours,
                parser_name=payload.parser_name.strip(),
                include_images=include_images,
                use_product_info=use_product_info,
                is_active=payload.is_active,
                created_at=now,
                updated_at=now,
            )
            session.add(task)
            session.commit()
            session.refresh(task)
            LOGGER.info(
                "Dashboard task created: id=%s store=%s parser=%s include_images=%s use_product_info=%s active=%s",
                task.id,
                task.store,
                task.parser_name,
                task.include_images,
                task.use_product_info,
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
                "Dashboard task updated: id=%s active=%s include_images=%s use_product_info=%s frequency_hours=%s parser=%s",
                task.id,
                task.is_active,
                task.include_images,
                task.use_product_info,
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

    @router.get("/api/store-directory")
    def list_store_directory(
        parser_name: str = Query(min_length=1, max_length=64),
        active_only: bool = Query(default=True),
    ) -> list[dict[str, object]]:
        normalized_parser = parser_name.strip().lower()
        session = session_factory()
        try:
            stmt = select(ParserStoreDirectory).where(ParserStoreDirectory.parser_name == normalized_parser)
            if active_only:
                stmt = stmt.where(ParserStoreDirectory.is_active.is_(True))
            rows = session.scalars(
                stmt.order_by(
                    func.coalesce(
                        ParserStoreDirectory.city_alias,
                        ParserStoreDirectory.city_name,
                        ParserStoreDirectory.store_code,
                    ).asc(),
                    ParserStoreDirectory.store_code.asc(),
                )
            ).all()
            return [_store_directory_to_dict(row) for row in rows]
        finally:
            session.close()

    @router.post("/api/store-directory/sync")
    async def sync_store_directory(payload: StoreDirectorySyncIn) -> dict[str, object]:
        parser_name = payload.parser_name.strip().lower()
        if parser_name not in SUPPORTED_STORE_DIRECTORY_PARSERS:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Parser {parser_name!r} is not supported in store-directory sync (v1).",
            )

        request_payload: dict[str, Any] = {
            "action": "collect_stores",
            "parser": parser_name,
        }
        if payload.country_id is not None:
            request_payload["country_id"] = int(payload.country_id)
        if payload.api_timeout_ms is not None:
            request_payload["api_timeout_ms"] = float(payload.api_timeout_ms)
        if payload.strict_validation is not None:
            request_payload["strict_validation"] = bool(payload.strict_validation)

        try:
            remote_response = await _orchestrator_ws_call(request_payload)
        except Exception as exc:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Store-directory sync failed: {exc}",
            ) from exc

        if remote_response.get("ok") is not True:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Store-directory sync failed: {remote_response.get('error', 'unknown error')}",
            )

        raw_stores = remote_response.get("stores")
        if not isinstance(raw_stores, list):
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Store-directory sync failed: parser returned invalid stores payload.",
            )

        warnings: list[str] = [
            str(item).strip()
            for item in remote_response.get("warnings", [])
            if isinstance(item, str) and item.strip()
        ]
        normalized_by_code: dict[str, dict[str, Any]] = {}
        skipped_count = 0
        duplicate_count = 0
        for raw_store in raw_stores:
            if not isinstance(raw_store, dict):
                skipped_count += 1
                warnings.append("store entry skipped: invalid payload item")
                continue
            normalized, warning = _normalize_store_directory_payload(
                parser_name=parser_name,
                raw_store=raw_store,
            )
            if warning is not None:
                skipped_count += 1
                warnings.append(warning)
                continue
            if normalized is None:
                skipped_count += 1
                continue
            store_code = str(normalized["store_code"])
            if store_code in normalized_by_code:
                duplicate_count += 1
            normalized_by_code[store_code] = normalized

        normalized_rows = list(normalized_by_code.values())
        partial_count = sum(1 for item in normalized_rows if bool(item.get("is_partial")))
        remote_count_raw = remote_response.get("stores_count")
        if isinstance(remote_count_raw, int) and not isinstance(remote_count_raw, bool):
            remote_count = max(0, remote_count_raw)
        elif isinstance(remote_count_raw, float):
            remote_count = max(0, int(remote_count_raw))
        elif isinstance(remote_count_raw, str):
            token = remote_count_raw.strip()
            if token.isdigit():
                remote_count = int(token)
            else:
                remote_count = len(raw_stores)
        else:
            remote_count = len(raw_stores)
        synced_at = utcnow()

        session = session_factory()
        try:
            active_before = int(
                session.scalar(
                    select(func.count(ParserStoreDirectory.id)).where(
                        ParserStoreDirectory.parser_name == parser_name,
                        ParserStoreDirectory.is_active.is_(True),
                    )
                )
                or 0
            )
            inserted_count = 0
            updated_count = 0
            reactivated_count = 0
            deactivated_count = 0

            if not normalized_rows:
                warnings.append(
                    "Parser returned empty store snapshot. Existing active directory records were kept unchanged."
                )
                active_after = active_before
            else:
                seen_codes = {str(item["store_code"]) for item in normalized_rows}
                existing_rows = session.scalars(
                    select(ParserStoreDirectory).where(
                        ParserStoreDirectory.parser_name == parser_name,
                        ParserStoreDirectory.store_code.in_(seen_codes),
                    )
                ).all()
                existing_by_code = {row.store_code: row for row in existing_rows}

                for item in normalized_rows:
                    store_code = str(item["store_code"])
                    row = existing_by_code.get(store_code)
                    if row is None:
                        session.add(
                            ParserStoreDirectory(
                                **item,
                                is_active=True,
                                first_seen_at=synced_at,
                                last_seen_at=synced_at,
                                updated_at=synced_at,
                            )
                        )
                        inserted_count += 1
                        continue

                    was_active = bool(row.is_active)
                    row.city_name = item.get("city_name")
                    row.city_alias = item.get("city_alias")
                    row.country = item.get("country")
                    row.region = item.get("region")
                    row.retail_type = item.get("retail_type")
                    row.address = item.get("address")
                    row.schedule_weekdays_open_from = item.get("schedule_weekdays_open_from")
                    row.schedule_weekdays_closed_from = item.get("schedule_weekdays_closed_from")
                    row.schedule_saturday_open_from = item.get("schedule_saturday_open_from")
                    row.schedule_saturday_closed_from = item.get("schedule_saturday_closed_from")
                    row.schedule_sunday_open_from = item.get("schedule_sunday_open_from")
                    row.schedule_sunday_closed_from = item.get("schedule_sunday_closed_from")
                    row.temporarily_closed = item.get("temporarily_closed")
                    row.longitude = item.get("longitude")
                    row.latitude = item.get("latitude")
                    row.payload_json = item.get("payload_json")
                    row.is_partial = bool(item.get("is_partial"))
                    row.is_active = True
                    row.last_seen_at = synced_at
                    row.updated_at = synced_at
                    if was_active:
                        updated_count += 1
                    else:
                        reactivated_count += 1

                deactivated_count = int(
                    session.execute(
                        update(ParserStoreDirectory)
                        .where(
                            ParserStoreDirectory.parser_name == parser_name,
                            ParserStoreDirectory.is_active.is_(True),
                            ParserStoreDirectory.store_code.notin_(seen_codes),
                        )
                        .values(
                            is_active=False,
                            updated_at=synced_at,
                        )
                        .execution_options(synchronize_session=False)
                    ).rowcount
                    or 0
                )
                session.commit()
                active_after = int(
                    session.scalar(
                        select(func.count(ParserStoreDirectory.id)).where(
                            ParserStoreDirectory.parser_name == parser_name,
                            ParserStoreDirectory.is_active.is_(True),
                        )
                    )
                    or 0
                )

            if duplicate_count > 0:
                warnings.append(
                    f"Duplicate store codes in parser payload were merged: {duplicate_count} entries."
                )

            return {
                "ok": True,
                "parser_name": parser_name,
                "synced_at": synced_at.isoformat(),
                "remote_count": remote_count,
                "processed_count": len(normalized_rows),
                "partial_count": partial_count,
                "skipped_count": skipped_count,
                "inserted_count": inserted_count,
                "updated_count": updated_count,
                "reactivated_count": reactivated_count,
                "deactivated_count": deactivated_count,
                "active_before": active_before,
                "active_after": active_after,
                "warnings": warnings,
            }
        finally:
            session.close()

    @router.post("/api/tasks/{task_id}/reset-last-crawl")
    def reset_task_last_crawl(task_id: int) -> dict[str, object]:
        session = session_factory()
        try:
            task = session.get(CrawlTask, task_id)
            if task is None or task.deleted_at is not None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")

            now = utcnow()
            task.last_crawl_at = now
            task.updated_at = now
            session.commit()
            session.refresh(task)
            LOGGER.info("Dashboard task last_crawl reset: id=%s at=%s", task.id, now.isoformat())
            return task_to_dict(task, now=now)
        finally:
            session.close()

    @router.post("/api/tasks/{task_id}/force-run")
    async def force_run_task(task_id: int) -> dict[str, object]:
        session = session_factory()
        try:
            task = session.get(CrawlTask, task_id)
            if task is None or task.deleted_at is not None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Task not found")

            has_assigned_run = session.scalar(
                select(func.count(TaskRun.id)).where(
                    TaskRun.task_id == task.id,
                    TaskRun.status == "assigned",
                )
            )
            if int(has_assigned_run or 0) > 0:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Task already has assigned run",
                )

            orchestrator = create_or_get_orchestrator(session, name=app_settings.orchestrator_manager_name)
            now = utcnow()
            run = TaskRun(
                id=uuid4().hex,
                task_id=task.id,
                orchestrator_id=orchestrator.id,
                status="assigned",
                assigned_at=now,
            )
            task.lease_owner_id = orchestrator.id
            task.lease_until = now + timedelta(minutes=max(1, int(app_settings.lease_ttl_minutes)))
            task.updated_at = now
            session.add(run)
            session.commit()
            session.refresh(run)

            request_payload: dict[str, Any] = {
                "action": "submit_store",
                "store_code": task.store,
                "parser": task.parser_name,
                "include_images": bool(task.include_images),
                "use_product_info": bool(task.use_product_info),
                "full_catalog": bool(app_settings.orchestrator_submit_full_catalog),
            }
            run_id = run.id
            orchestrator_id = orchestrator.id
        finally:
            session.close()

        remote_response: dict[str, Any]
        try:
            remote_response = await _orchestrator_ws_call(request_payload)
        except Exception as exc:
            remote_response = {"ok": False, "error": str(exc)}

        session = session_factory()
        try:
            run = session.get(TaskRun, run_id)
            orchestrator = session.get(Orchestrator, orchestrator_id)
            if run is None or orchestrator is None:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Run/orchestrator not found")

            if remote_response.get("ok") and isinstance(remote_response.get("job_id"), str):
                run.dispatch_meta_json = {
                    "submitted_at": utcnow().isoformat(),
                    "remote_job_id": str(remote_response.get("job_id")),
                    "remote_status": remote_response.get("status"),
                    "request": dict(request_payload),
                    "response": remote_response,
                }
                session.commit()
                session.refresh(run)
                LOGGER.info(
                    "Force-run submitted: task_id=%s run_id=%s remote_job_id=%s",
                    task_id,
                    run.id,
                    remote_response.get("job_id"),
                )
                return {
                    "ok": True,
                    "task_id": task_id,
                    "run_id": run.id,
                    "remote_job_id": str(remote_response.get("job_id")),
                    "status": "submitted",
                }

            submit_error = str(remote_response.get("error", "submit_store failed"))
            run.dispatch_meta_json = {
                "submit_error": submit_error,
                "last_submit_attempt_at": utcnow().isoformat(),
                "request": dict(request_payload),
                "response": remote_response,
            }
            finish_run(
                session,
                run=run,
                orchestrator=orchestrator,
                status="error",
                processed_images=int(run.processed_images or 0),
                error_message=f"Submit failed: {submit_error}",
            )
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Submit failed: {submit_error}",
            )
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
            request_payload = {
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

    @router.post("/api/runs/{run_id}/cancel")
    async def cancel_run(run_id: str) -> dict[str, object]:
        session = session_factory()
        try:
            run = session.get(TaskRun, run_id)
            if run is None:
                LOGGER.warning("Run cancel failed: run_id=%s reason=run_not_found", run_id)
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")

            if str(run.status).strip().lower() in TERMINAL_RUN_STATUSES:
                return {
                    "ok": True,
                    "run_id": run_id,
                    "status": "already_terminal",
                    "run_status": str(run.status),
                }

            run_meta = dispatch_meta(run.dispatch_meta_json)
            remote_job_id = run_meta.get("remote_job_id")
            if not isinstance(remote_job_id, str) or not remote_job_id.strip():
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Run is not linked to orchestrator WS job",
                )
            remote_job_id = remote_job_id.strip()
            orchestrator_id = run.orchestrator_id
        finally:
            session.close()

        parser_socket: Any | None = None
        cancel_response: dict[str, Any]
        try:
            connector = connect_orchestrator_ws_getter()
            del connector
            cancel_response = await _orchestrator_ws_call(
                {
                    "action": "cancel_job",
                    "job_id": remote_job_id,
                }
            )
        except HTTPException:
            raise
        except Exception as exc:
            LOGGER.exception("Run cancel request failed: run_id=%s error=%s", run_id, exc)
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Cancel request failed: {exc}",
            ) from exc

        if cancel_response.get("ok") is not True:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=str(cancel_response.get("error", "Orchestrator cancel failed")),
            )

        session = session_factory()
        try:
            run = session.get(TaskRun, run_id)
            if run is None:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Run not found")
            orchestrator = session.get(Orchestrator, orchestrator_id)
            if orchestrator is None:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Run orchestrator not found")

            run_meta = dict(dispatch_meta(run.dispatch_meta_json))
            remote_status = str(cancel_response.get("status", "cancelled")).strip().lower() or "cancelled"
            run_meta.update(
                {
                    "remote_status": remote_status,
                    "cancelled_at": utcnow().isoformat(),
                    "cancel_request": {"action": "cancel_job", "job_id": remote_job_id},
                    "cancel_response": cancel_response,
                }
            )
            run.dispatch_meta_json = run_meta

            if (
                remote_status == "cancelled"
                and str(run.status).strip().lower() not in TERMINAL_RUN_STATUSES
            ):
                finish_run(
                    session,
                    run=run,
                    orchestrator=orchestrator,
                    status="error",
                    processed_images=int(run.processed_images or 0),
                    error_message="Cancelled from dashboard",
                )
            else:
                session.commit()

            LOGGER.info(
                "Run cancelled from dashboard: run_id=%s remote_job_id=%s",
                run_id,
                remote_job_id,
            )
            return {
                "ok": True,
                "run_id": run_id,
                "remote_job_id": remote_job_id,
                "status": remote_status,
            }
        finally:
            session.close()

    @router.get("/api/overview")
    def overview() -> dict[str, object]:
        def _non_negative_int_or_none(value: Any) -> int | None:
            if isinstance(value, bool):
                return None
            if isinstance(value, int):
                return max(0, value)
            if isinstance(value, float):
                if value.is_integer():
                    return max(0, int(value))
                return None
            if isinstance(value, str):
                token = value.strip()
                if not token:
                    return None
                try:
                    return max(0, int(token))
                except ValueError:
                    return None
            return None

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
                    .where(
                        TaskRun.status == "success",
                        TaskRun.artifact_dataclass_validated.is_(False),
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
                    TaskRun.finish,
                    TaskRun.error_message,
                    Orchestrator.name,
                    CrawlTask.city,
                    CrawlTask.store,
                    TaskRun.processed_images,
                    TaskRun.artifact_products_count,
                    TaskRun.artifact_categories_count,
                    TaskRun.converter_elapsed_sec,
                    TaskRun.dispatch_meta_json,
                    TaskRun.artifact_dataclass_validated,
                )
                .join(Orchestrator, Orchestrator.id == TaskRun.orchestrator_id)
                .join(CrawlTask, CrawlTask.id == TaskRun.task_id)
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
                converter_finish,
                run_error_message,
                orchestrator_name,
                city,
                store,
                processed_images,
                artifact_products_count,
                artifact_categories_count,
                converter_elapsed_sec,
                run_dispatch_meta,
                dataclass_validated,
            ) in recent_rows:
                run_meta = dispatch_meta(run_dispatch_meta)
                remote_status = str(run_meta.get("remote_status", "")).strip().lower() or None
                remote_terminal = remote_status in {"success", "error", "cancelled"}
                remote_job_id = run_meta.get("remote_job_id")
                has_remote_job_id = isinstance(remote_job_id, str) and bool(remote_job_id.strip())
                progress_payload = run_meta.get("category_progress")
                progress_total: int | None = None
                progress_done: int | None = None
                progress_alias: str | None = None
                progress_percent: int | None = None
                if isinstance(progress_payload, dict):
                    progress_total = _non_negative_int_or_none(progress_payload.get("categories_total"))
                    progress_done = _non_negative_int_or_none(progress_payload.get("categories_done"))
                    if progress_total is not None and progress_done is not None:
                        if progress_total > 0:
                            progress_done = min(progress_done, progress_total)
                            progress_percent = int(round((progress_done / progress_total) * 100))
                        elif progress_done > 0:
                            progress_total = progress_done
                            progress_percent = 100
                        else:
                            progress_percent = 0
                    alias_token = progress_payload.get("current_category_alias")
                    if alias_token is not None:
                        normalized_alias = str(alias_token).strip()
                        progress_alias = normalized_alias or None
                local_terminal = str(run_status).strip().lower() in {"success", "error"}
                validation_failed = bool(run_status == "success" and dataclass_validated is False)
                if validation_failed:
                    display_status = "validation_failed"
                elif str(run_status).strip().lower() == "error" and remote_status == "cancelled":
                    display_status = "cancelled"
                else:
                    display_status = str(run_status)
                recent_runs.append(
                    {
                        "id": run_id,
                        "task_id": task_id,
                        "status": run_status,
                        "display_status": display_status,
                        "assigned_at": as_utc(assigned_at).isoformat(),
                        "finished_at": as_utc(finished_at).isoformat() if finished_at else None,
                        "finish": as_utc(converter_finish).isoformat() if converter_finish else None,
                        "orchestrator_name": orchestrator_name,
                        "city": city,
                        "store": store,
                        "processed_images": int(processed_images or 0),
                        "artifact_products_count": int(artifact_products_count or 0),
                        "artifact_categories_count": int(artifact_categories_count or 0),
                        "converter_elapsed_sec": int(converter_elapsed_sec or 0),
                        "remote_status": remote_status,
                        "remote_terminal": bool(remote_terminal),
                        "error_message": str(run_error_message).strip() if run_error_message else None,
                        "progress_total": progress_total,
                        "progress_done": progress_done,
                        "progress_category_alias": progress_alias,
                        "progress_percent": progress_percent,
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

    @router.get("/api/validation-errors")
    def validation_errors() -> dict[str, object]:
        session = session_factory()
        try:
            now = utcnow()
            rows = session.execute(
                select(
                    TaskRun.task_id,
                    TaskRun.id,
                    TaskRun.assigned_at,
                    TaskRun.finished_at,
                    CrawlTask.city,
                    CrawlTask.store,
                    CrawlTask.parser_name,
                    CrawlTask.is_active,
                    TaskRun.artifact_dataclass_validated,
                    TaskRun.artifact_dataclass_validation_error,
                    TaskRun.dispatch_meta_json,
                )
                .join(CrawlTask, CrawlTask.id == TaskRun.task_id)
                .where(
                    CrawlTask.deleted_at.is_(None),
                    TaskRun.status == "success",
                    TaskRun.artifact_ingested_at.is_not(None),
                )
                .order_by(
                    TaskRun.task_id.asc(),
                    TaskRun.assigned_at.desc(),
                    TaskRun.id.desc(),
                )
            ).all()

            # For each task we keep only the latest successful run.
            # If that latest run still fails dataclass validation, it is unresolved.
            scanned_task_ids: set[int] = set()
            unresolved: list[dict[str, object]] = []
            for (
                task_id,
                run_id,
                assigned_at,
                finished_at,
                city,
                store,
                parser_name,
                is_active,
                dataclass_validated,
                dataclass_validation_error,
                dispatch_meta_json,
            ) in rows:
                normalized_task_id = int(task_id)
                if normalized_task_id in scanned_task_ids:
                    continue
                scanned_task_ids.add(normalized_task_id)
                if dataclass_validated is not False:
                    continue

                run_meta = dispatch_meta(dispatch_meta_json)
                remote_status = str(run_meta.get("remote_status", "")).strip().lower() or None
                unresolved.append(
                    {
                        "task_id": normalized_task_id,
                        "run_id": str(run_id),
                        "city": city,
                        "store": store,
                        "parser_name": parser_name,
                        "is_active": bool(is_active),
                        "assigned_at": as_utc(assigned_at).isoformat() if assigned_at else None,
                        "finished_at": as_utc(finished_at).isoformat() if finished_at else None,
                        "remote_status": remote_status,
                        "dataclass_validation_error": dataclass_validation_error,
                    }
                )

            unresolved.sort(key=lambda item: str(item.get("assigned_at") or ""), reverse=True)
            LOGGER.debug(
                "Dashboard validation errors listed: unresolved=%s scanned_tasks=%s",
                len(unresolved),
                len(scanned_task_ids),
            )
            return {
                "generated_at": now.isoformat(),
                "total": len(unresolved),
                "items": unresolved,
            }
        finally:
            session.close()

    return router
