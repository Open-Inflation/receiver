from __future__ import annotations

import logging

from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

LOGGER = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


def create_sqlalchemy_engine(database_url: str):
    connect_args: dict[str, object] = {}
    if database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False

    engine = create_engine(
        database_url,
        pool_pre_ping=True,
        future=True,
        connect_args=connect_args,
    )
    safe_database_url = make_url(database_url).render_as_string(hide_password=True)
    LOGGER.info("SQLAlchemy engine created: database_url=%s", safe_database_url)
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
