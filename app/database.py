from __future__ import annotations

import logging
import os

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

LOGGER = logging.getLogger(__name__)
_POSTGRES_DRIVERS = {
    "postgres",
    "postgresql",
    "postgres+psycopg",
    "postgresql+psycopg",
}


class Base(DeclarativeBase):
    pass


def _env_int(name: str, default: int, *, minimum: int | None = None) -> int:
    raw = os.getenv(name)
    if raw is None:
        value = default
    else:
        try:
            value = int(raw)
        except ValueError:
            value = default
    if minimum is not None and value < minimum:
        return minimum
    return value


def _env_float(name: str, default: float, *, minimum: float | None = None) -> float:
    raw = os.getenv(name)
    if raw is None:
        value = default
    else:
        try:
            value = float(raw)
        except ValueError:
            value = default
    if minimum is not None and value < minimum:
        return minimum
    return value


def normalize_database_url(database_url: str) -> str:
    token = (database_url or "").strip()
    if not token:
        raise ValueError("DATABASE_URL is empty")

    url = make_url(token)
    drivername = (url.drivername or "").lower()
    if drivername.startswith("sqlite"):
        return token

    if drivername not in _POSTGRES_DRIVERS:
        raise ValueError(
            f"Unsupported DATABASE_URL driver: {url.drivername!r}. "
            "Only PostgreSQL (postgresql+psycopg://...) or SQLite is supported."
        )

    if drivername != "postgresql+psycopg":
        url = url.set(drivername="postgresql+psycopg")
    return url.render_as_string(hide_password=False)


def create_sqlalchemy_engine(database_url: str):
    normalized_database_url = normalize_database_url(database_url)
    connect_args: dict[str, object] = {}
    engine_kwargs: dict[str, object] = {}
    if make_url(normalized_database_url).drivername.startswith("sqlite"):
        connect_args["check_same_thread"] = False
    else:
        engine_kwargs["pool_size"] = _env_int("DB_POOL_SIZE", 10, minimum=1)
        engine_kwargs["max_overflow"] = _env_int("DB_MAX_OVERFLOW", 20, minimum=0)
        engine_kwargs["pool_timeout"] = _env_float("DB_POOL_TIMEOUT_SEC", 30.0, minimum=1.0)
        engine_kwargs["pool_recycle"] = _env_int("DB_POOL_RECYCLE_SEC", 1800, minimum=1)

    engine = create_engine(
        normalized_database_url,
        pool_pre_ping=True,
        future=True,
        connect_args=connect_args,
        **engine_kwargs,
    )
    safe_database_url = make_url(normalized_database_url).render_as_string(hide_password=True)
    LOGGER.info(
        "SQLAlchemy engine created: database_url=%s pool_size=%s max_overflow=%s pool_timeout=%s pool_recycle=%s",
        safe_database_url,
        engine_kwargs.get("pool_size", "sqlite-default"),
        engine_kwargs.get("max_overflow", "sqlite-default"),
        engine_kwargs.get("pool_timeout", "sqlite-default"),
        engine_kwargs.get("pool_recycle", "sqlite-default"),
    )
    return engine


def create_session_factory(engine) -> sessionmaker[Session]:
    factory = sessionmaker(
        bind=engine,
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
        class_=Session,
    )
    LOGGER.debug("SQLAlchemy session factory created")
    return factory


def ensure_task_runs_runtime_columns(engine) -> None:
    inspector = inspect(engine)
    dialect = engine.dialect.name
    executed_task_runs = 0
    executed_crawl_tasks = 0
    executed_store_directory = 0
    executed_run_artifacts = 0

    with engine.begin() as connection:
        if inspector.has_table("task_runs"):
            existing_task_run_columns = {str(item.get("name")) for item in inspector.get_columns("task_runs")}
            if dialect == "postgresql":
                task_runs_ddl = {
                    "converter_elapsed_sec": "ALTER TABLE task_runs ADD COLUMN converter_elapsed_sec BIGINT DEFAULT 0",
                    "finish": "ALTER TABLE task_runs ADD COLUMN finish TIMESTAMPTZ",
                    "artifact_source": "ALTER TABLE task_runs ADD COLUMN artifact_source VARCHAR(32)",
                    "artifact_products_count": "ALTER TABLE task_runs ADD COLUMN artifact_products_count BIGINT DEFAULT 0",
                    "artifact_categories_count": "ALTER TABLE task_runs ADD COLUMN artifact_categories_count BIGINT DEFAULT 0",
                    "artifact_dataclass_validated": "ALTER TABLE task_runs ADD COLUMN artifact_dataclass_validated BOOLEAN",
                    "artifact_dataclass_validation_error": "ALTER TABLE task_runs ADD COLUMN artifact_dataclass_validation_error TEXT",
                    "artifact_ingested_at": "ALTER TABLE task_runs ADD COLUMN artifact_ingested_at TIMESTAMPTZ",
                }
            else:
                task_runs_ddl = {
                    "converter_elapsed_sec": "ALTER TABLE task_runs ADD COLUMN converter_elapsed_sec INTEGER DEFAULT 0",
                    "finish": "ALTER TABLE task_runs ADD COLUMN finish TEXT",
                    "artifact_source": "ALTER TABLE task_runs ADD COLUMN artifact_source TEXT",
                    "artifact_products_count": "ALTER TABLE task_runs ADD COLUMN artifact_products_count INTEGER DEFAULT 0",
                    "artifact_categories_count": "ALTER TABLE task_runs ADD COLUMN artifact_categories_count INTEGER DEFAULT 0",
                    "artifact_dataclass_validated": "ALTER TABLE task_runs ADD COLUMN artifact_dataclass_validated INTEGER",
                    "artifact_dataclass_validation_error": "ALTER TABLE task_runs ADD COLUMN artifact_dataclass_validation_error TEXT",
                    "artifact_ingested_at": "ALTER TABLE task_runs ADD COLUMN artifact_ingested_at TEXT",
                }

            for column_name, statement in task_runs_ddl.items():
                if column_name in existing_task_run_columns:
                    continue
                connection.execute(text(statement))
                executed_task_runs += 1

            connection.execute(
                text(
                    "UPDATE task_runs "
                    "SET converter_elapsed_sec = COALESCE(converter_elapsed_sec, 0), "
                    "artifact_products_count = COALESCE(artifact_products_count, 0), "
                    "artifact_categories_count = COALESCE(artifact_categories_count, 0)"
                )
            )

        if inspector.has_table("crawl_tasks"):
            existing_task_columns = {str(item.get("name")) for item in inspector.get_columns("crawl_tasks")}
            if "use_product_info" not in existing_task_columns:
                if dialect == "postgresql":
                    connection.execute(
                        text("ALTER TABLE crawl_tasks ADD COLUMN use_product_info BOOLEAN DEFAULT TRUE")
                    )
                else:
                    connection.execute(
                        text("ALTER TABLE crawl_tasks ADD COLUMN use_product_info INTEGER DEFAULT 1")
                    )
                executed_crawl_tasks += 1

            bool_default = "TRUE" if dialect == "postgresql" else "1"
            connection.execute(
                text(f"UPDATE crawl_tasks SET use_product_info = COALESCE(use_product_info, {bool_default})")
            )

        if inspector.has_table("parser_store_directory"):
            existing_store_directory_columns = {
                str(item.get("name")) for item in inspector.get_columns("parser_store_directory")
            }
            if dialect == "postgresql":
                store_directory_ddl = {
                    "rating": "ALTER TABLE parser_store_directory ADD COLUMN rating NUMERIC(4, 2)",
                    "reviews_count": "ALTER TABLE parser_store_directory ADD COLUMN reviews_count INTEGER",
                    "open_date": "ALTER TABLE parser_store_directory ADD COLUMN open_date VARCHAR(32)",
                }
            else:
                store_directory_ddl = {
                    "rating": "ALTER TABLE parser_store_directory ADD COLUMN rating REAL",
                    "reviews_count": "ALTER TABLE parser_store_directory ADD COLUMN reviews_count INTEGER",
                    "open_date": "ALTER TABLE parser_store_directory ADD COLUMN open_date TEXT",
                }

            for column_name, statement in store_directory_ddl.items():
                if column_name in existing_store_directory_columns:
                    continue
                connection.execute(text(statement))
                executed_store_directory += 1

        if inspector.has_table("run_artifacts"):
            existing_run_artifact_columns = {
                str(item.get("name")) for item in inspector.get_columns("run_artifacts")
            }
            if dialect == "postgresql":
                run_artifacts_ddl = {
                    "rating": "ALTER TABLE run_artifacts ADD COLUMN rating NUMERIC(4, 2)",
                    "reviews_count": "ALTER TABLE run_artifacts ADD COLUMN reviews_count INTEGER",
                    "open_date": "ALTER TABLE run_artifacts ADD COLUMN open_date VARCHAR(32)",
                }
            else:
                run_artifacts_ddl = {
                    "rating": "ALTER TABLE run_artifacts ADD COLUMN rating REAL",
                    "reviews_count": "ALTER TABLE run_artifacts ADD COLUMN reviews_count INTEGER",
                    "open_date": "ALTER TABLE run_artifacts ADD COLUMN open_date TEXT",
                }

            for column_name, statement in run_artifacts_ddl.items():
                if column_name in existing_run_artifact_columns:
                    continue
                connection.execute(text(statement))
                executed_run_artifacts += 1

    if executed_task_runs:
        LOGGER.info("Receiver runtime schema reconciled for task_runs: added_columns=%s", executed_task_runs)
    if executed_crawl_tasks:
        LOGGER.info("Receiver runtime schema reconciled for crawl_tasks: added_columns=%s", executed_crawl_tasks)
    if executed_store_directory:
        LOGGER.info(
            "Receiver runtime schema reconciled for parser_store_directory: added_columns=%s",
            executed_store_directory,
        )
    if executed_run_artifacts:
        LOGGER.info(
            "Receiver runtime schema reconciled for run_artifacts: added_columns=%s",
            executed_run_artifacts,
        )
