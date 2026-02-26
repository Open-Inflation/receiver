from __future__ import annotations

import logging
import os

from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

LOGGER = logging.getLogger(__name__)


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


def create_sqlalchemy_engine(database_url: str):
    connect_args: dict[str, object] = {}
    engine_kwargs: dict[str, object] = {}
    if database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
    else:
        engine_kwargs["pool_size"] = _env_int("DB_POOL_SIZE", 10, minimum=1)
        engine_kwargs["max_overflow"] = _env_int("DB_MAX_OVERFLOW", 20, minimum=0)
        engine_kwargs["pool_timeout"] = _env_float("DB_POOL_TIMEOUT_SEC", 30.0, minimum=1.0)
        engine_kwargs["pool_recycle"] = _env_int("DB_POOL_RECYCLE_SEC", 1800, minimum=1)

    engine = create_engine(
        database_url,
        pool_pre_ping=True,
        future=True,
        connect_args=connect_args,
        **engine_kwargs,
    )
    safe_database_url = make_url(database_url).render_as_string(hide_password=True)
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
