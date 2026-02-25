from __future__ import annotations

import logging
from collections.abc import Iterable

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError


LOGGER = logging.getLogger(__name__)


TASK_RUNS_COMPAT_COLUMNS: dict[str, str] = {
    "image_results_json": "TEXT",
    "output_json": "TEXT",
    "output_gz": "TEXT",
    "download_url": "TEXT",
    "download_sha256": "VARCHAR(128)",
    "download_expires_at": "DATETIME",
}


def _is_duplicate_column_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "duplicate column" in message
        or "already exists" in message
        or "duplicate column name" in message
    )


def _apply_columns_patch(engine: Engine, table_name: str, columns: Iterable[tuple[str, str]]) -> list[str]:
    applied: list[str] = []
    with engine.begin() as connection:
        for column_name, column_type in columns:
            statement = f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
            try:
                connection.execute(text(statement))
                applied.append(column_name)
            except DBAPIError as exc:
                if _is_duplicate_column_error(exc):
                    continue
                raise
    return applied


def apply_compat_schema_patches(engine: Engine) -> None:
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())

    if "task_runs" not in tables:
        return

    existing_columns = {column["name"] for column in inspector.get_columns("task_runs")}
    missing_columns = [
        (column_name, column_type)
        for column_name, column_type in TASK_RUNS_COMPAT_COLUMNS.items()
        if column_name not in existing_columns
    ]

    if not missing_columns:
        return

    applied = _apply_columns_patch(engine, "task_runs", missing_columns)
    if applied:
        LOGGER.warning(
            "Applied compatibility schema patch to task_runs: added columns %s",
            ", ".join(applied),
        )
