from __future__ import annotations

from pathlib import Path
from datetime import datetime
from typing import Any

from sqlalchemy.engine import make_url

from app.models import CrawlTask
from app.services.scheduler import as_utc, is_task_due, utcnow


def ensure_sqlite_parent_dir(database_url: str) -> None:
    url = make_url(database_url)
    if not url.drivername.startswith("sqlite"):
        return

    if not url.database or url.database == ":memory:":
        return

    db_path = Path(url.database).expanduser()
    if not db_path.is_absolute():
        db_path = (Path.cwd() / db_path).resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)


def dispatch_meta(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    return payload


def _iso_utc(value: datetime | None) -> str | None:
    normalized = as_utc(value)
    if normalized is None:
        return None
    return normalized.isoformat()


def task_to_dict(task: CrawlTask, *, now: datetime | None = None) -> dict[str, object]:
    reference_time = now or utcnow()
    return {
        "id": task.id,
        "city": task.city,
        "store": task.store,
        "frequency_hours": task.frequency_hours,
        "last_crawl_at": _iso_utc(task.last_crawl_at),
        "parser_name": task.parser_name,
        "include_images": task.include_images,
        "use_product_info": task.use_product_info,
        "is_active": task.is_active,
        "is_due": bool(task.is_active and is_task_due(task, now=reference_time)),
        "lease_owner_id": task.lease_owner_id,
        "lease_until": _iso_utc(task.lease_until),
        "created_at": _iso_utc(task.created_at),
        "updated_at": _iso_utc(task.updated_at),
    }
