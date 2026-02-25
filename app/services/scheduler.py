from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import or_, select, update
from sqlalchemy.orm import Session

from ..models import CrawlTask, Orchestrator, TaskRun


TERMINAL_RUN_STATUSES = {"success", "error"}


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def as_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def is_task_due(task: CrawlTask, now: datetime | None = None) -> bool:
    reference_time = now or utcnow()

    last_crawl_at = as_utc(task.last_crawl_at)
    if last_crawl_at is None:
        return True
    due_at = last_crawl_at + timedelta(hours=task.frequency_hours)
    return due_at <= reference_time


def create_or_get_orchestrator(session: Session, *, name: str) -> Orchestrator:
    orchestrator = session.scalar(select(Orchestrator).where(Orchestrator.name == name))
    if orchestrator is not None:
        touch_orchestrator(session, orchestrator, commit=True)
        return orchestrator

    now = utcnow()
    orchestrator = Orchestrator(
        id=uuid4().hex,
        name=name,
        token=secrets.token_urlsafe(32),
        created_at=now,
        updated_at=now,
        last_heartbeat_at=now,
    )
    session.add(orchestrator)
    session.commit()
    session.refresh(orchestrator)
    return orchestrator


def touch_orchestrator(session: Session, orchestrator: Orchestrator, *, commit: bool = True) -> None:
    now = utcnow()
    orchestrator.last_heartbeat_at = now
    orchestrator.updated_at = now
    if commit:
        session.commit()


def claim_next_due_task(
    session: Session,
    *,
    orchestrator: Orchestrator,
    lease_ttl_minutes: int,
) -> tuple[CrawlTask, TaskRun] | None:
    now = utcnow()
    lease_until = now + timedelta(minutes=max(1, lease_ttl_minutes))

    candidates = session.scalars(
        select(CrawlTask)
        .where(CrawlTask.is_active.is_(True))
        .order_by(CrawlTask.last_crawl_at.asc().nullsfirst(), CrawlTask.id.asc())
    ).all()

    for candidate in candidates:
        lease_until_candidate = as_utc(candidate.lease_until)
        if lease_until_candidate is not None and lease_until_candidate > now:
            continue
        if not is_task_due(candidate, now=now):
            continue

        updated = session.execute(
            update(CrawlTask)
            .where(
                CrawlTask.id == candidate.id,
                CrawlTask.is_active.is_(True),
                or_(CrawlTask.lease_until.is_(None), CrawlTask.lease_until <= now),
            )
            .values(
                lease_owner_id=orchestrator.id,
                lease_until=lease_until,
                updated_at=now,
            )
        ).rowcount

        if updated != 1:
            continue

        run = TaskRun(
            id=uuid4().hex,
            task_id=candidate.id,
            orchestrator_id=orchestrator.id,
            status="assigned",
            assigned_at=now,
        )
        session.add(run)
        session.commit()

        claimed_task = session.get(CrawlTask, candidate.id)
        session.refresh(run)
        if claimed_task is None:
            return None
        return claimed_task, run

    return None


def finish_run(
    session: Session,
    *,
    run: TaskRun,
    orchestrator: Orchestrator,
    status: str,
    payload: dict | None,
    parser_payload: dict | None,
    image_results: list[dict],
    output_json: str | None,
    output_gz: str | None,
    download_url: str | None,
    download_sha256: str | None,
    download_expires_at: datetime | None,
    error_message: str | None,
) -> TaskRun:
    if run.status in TERMINAL_RUN_STATUSES:
        return run

    now = utcnow()
    run.status = status
    run.finished_at = now
    run.payload_json = payload
    run.parser_payload_json = parser_payload
    run.image_results_json = image_results
    run.output_json = output_json
    run.output_gz = output_gz
    run.download_url = download_url
    run.download_sha256 = download_sha256
    run.download_expires_at = download_expires_at
    run.error_message = error_message

    task = session.get(CrawlTask, run.task_id)
    if task is not None:
        if status == "success":
            task.last_crawl_at = now
        if task.lease_owner_id == orchestrator.id:
            task.lease_owner_id = None
            task.lease_until = None
        task.updated_at = now

    orchestrator.last_heartbeat_at = now
    orchestrator.updated_at = now

    session.commit()
    session.refresh(run)
    return run
