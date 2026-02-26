from __future__ import annotations

import logging
import secrets
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import case, or_, select, update
from sqlalchemy.orm import Session

from ..models import CrawlTask, Orchestrator, TaskRun


TERMINAL_RUN_STATUSES = {"success", "error"}
LOGGER = logging.getLogger(__name__)


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
        LOGGER.debug("Orchestrator reused: id=%s name=%s", orchestrator.id, orchestrator.name)
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
    LOGGER.info("Orchestrator created: id=%s name=%s", orchestrator.id, orchestrator.name)
    return orchestrator


def touch_orchestrator(session: Session, orchestrator: Orchestrator, *, commit: bool = True) -> None:
    now = utcnow()
    orchestrator.last_heartbeat_at = now
    orchestrator.updated_at = now
    if commit:
        session.commit()
    LOGGER.debug("Orchestrator touched: id=%s commit=%s", orchestrator.id, commit)


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
        .where(CrawlTask.is_active.is_(True), CrawlTask.deleted_at.is_(None))
        .order_by(
            case((CrawlTask.last_crawl_at.is_(None), 0), else_=1).asc(),
            CrawlTask.last_crawl_at.asc(),
            CrawlTask.id.asc(),
        )
    ).all()
    LOGGER.debug(
        "Claim scan started: orchestrator_id=%s candidates=%s lease_ttl_minutes=%s",
        orchestrator.id,
        len(candidates),
        lease_ttl_minutes,
    )
    skipped_leased = 0
    skipped_not_due = 0
    skipped_update_conflict = 0

    for candidate in candidates:
        lease_until_candidate = as_utc(candidate.lease_until)
        if lease_until_candidate is not None and lease_until_candidate > now:
            skipped_leased += 1
            continue
        if not is_task_due(candidate, now=now):
            skipped_not_due += 1
            continue

        updated = session.execute(
            update(CrawlTask)
            .where(
                CrawlTask.id == candidate.id,
                CrawlTask.is_active.is_(True),
                CrawlTask.deleted_at.is_(None),
                or_(CrawlTask.lease_until.is_(None), CrawlTask.lease_until <= now),
            )
            .values(
                lease_owner_id=orchestrator.id,
                lease_until=lease_until,
                updated_at=now,
            )
            # SQLite returns naive datetimes for DateTime(timezone=True) columns.
            # Avoid Python-side criteria evaluation during session sync, which can
            # raise on naive/aware datetime comparisons.
            .execution_options(synchronize_session=False)
        ).rowcount

        if updated != 1:
            skipped_update_conflict += 1
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
            LOGGER.warning("Claimed task disappeared after commit: task_id=%s run_id=%s", candidate.id, run.id)
            return None
        LOGGER.info(
            "Task claimed: task_id=%s run_id=%s orchestrator_id=%s lease_until=%s",
            claimed_task.id,
            run.id,
            orchestrator.id,
            lease_until.isoformat(),
        )
        return claimed_task, run

    LOGGER.debug(
        "No due task claimed: orchestrator_id=%s skipped_leased=%s skipped_not_due=%s skipped_update_conflict=%s",
        orchestrator.id,
        skipped_leased,
        skipped_not_due,
        skipped_update_conflict,
    )
    return None


def finish_run(
    session: Session,
    *,
    run: TaskRun,
    orchestrator: Orchestrator,
    status: str,
    processed_images: int,
    error_message: str | None,
) -> TaskRun:
    if run.status in TERMINAL_RUN_STATUSES:
        LOGGER.debug("Run already terminal: run_id=%s status=%s", run.id, run.status)
        return run

    now = utcnow()
    run.status = status
    run.finished_at = now
    run.processed_images = max(0, int(processed_images))
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
    LOGGER.info(
        "Run finished: run_id=%s task_id=%s orchestrator_id=%s status=%s processed_images=%s",
        run.id,
        run.task_id,
        orchestrator.id,
        run.status,
        run.processed_images,
    )
    return run
