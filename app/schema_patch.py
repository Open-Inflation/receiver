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

TASK_RUNS_COMPAT_INDEXES: dict[str, str] = {
    "ix_task_runs_assigned_at": "CREATE INDEX ix_task_runs_assigned_at ON task_runs (assigned_at)",
}

CRAWL_TASKS_COMPAT_COLUMNS: dict[str, str] = {
    "deleted_at": "DATETIME",
}


def _is_duplicate_schema_object_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return (
        "duplicate column" in message
        or "already exists" in message
        or "duplicate column name" in message
        or "duplicate key name" in message
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
                if _is_duplicate_schema_object_error(exc):
                    continue
                raise
    return applied


def _apply_indexes_patch(engine: Engine, indexes: Iterable[tuple[str, str]]) -> list[str]:
    applied: list[str] = []
    with engine.begin() as connection:
        for index_name, statement in indexes:
            try:
                connection.execute(text(statement))
                applied.append(index_name)
            except DBAPIError as exc:
                if _is_duplicate_schema_object_error(exc):
                    continue
                raise
    return applied


def apply_compat_schema_patches(engine: Engine) -> None:
    inspector = inspect(engine)
    tables = set(inspector.get_table_names())

    table_to_columns: dict[str, dict[str, str]] = {
        "task_runs": TASK_RUNS_COMPAT_COLUMNS,
        "crawl_tasks": CRAWL_TASKS_COMPAT_COLUMNS,
    }
    table_to_indexes: dict[str, dict[str, str]] = {
        "task_runs": TASK_RUNS_COMPAT_INDEXES,
    }

    for table_name, compat_columns in table_to_columns.items():
        if table_name not in tables:
            continue

        existing_columns = {column["name"] for column in inspector.get_columns(table_name)}
        missing_columns = [
            (column_name, column_type)
            for column_name, column_type in compat_columns.items()
            if column_name not in existing_columns
        ]
        if not missing_columns:
            continue

        applied = _apply_columns_patch(engine, table_name, missing_columns)
        if applied:
            LOGGER.warning(
                "Applied compatibility schema patch to %s: added columns %s",
                table_name,
                ", ".join(applied),
            )

    for table_name, compat_indexes in table_to_indexes.items():
        if table_name not in tables:
            continue

        existing_indexes = {index["name"] for index in inspector.get_indexes(table_name)}
        missing_indexes = [
            (index_name, statement)
            for index_name, statement in compat_indexes.items()
            if index_name not in existing_indexes
        ]
        if not missing_indexes:
            continue

        applied = _apply_indexes_patch(engine, missing_indexes)
        if applied:
            LOGGER.warning(
                "Applied compatibility schema patch to %s: added indexes %s",
                table_name,
                ", ".join(applied),
            )
