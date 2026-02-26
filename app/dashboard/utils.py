from __future__ import annotations

from pathlib import Path
from typing import Any

from sqlalchemy.engine import make_url

from app.models import CrawlTask


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


def task_to_dict(task: CrawlTask) -> dict[str, object]:
    return {
        "id": task.id,
        "city": task.city,
        "store": task.store,
        "frequency_hours": task.frequency_hours,
        "last_crawl_at": task.last_crawl_at.isoformat() if task.last_crawl_at else None,
        "parser_name": task.parser_name,
        "is_active": task.is_active,
        "lease_owner_id": task.lease_owner_id,
        "lease_until": task.lease_until.isoformat() if task.lease_until else None,
        "created_at": task.created_at.isoformat(),
        "updated_at": task.updated_at.isoformat(),
    }
