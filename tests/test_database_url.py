from __future__ import annotations

import pytest
from sqlalchemy.dialects import postgresql

from app.database import normalize_database_url
from app.models import CrawlTask, RunArtifact, RunArtifactProduct, TaskRun


def test_normalize_database_url_accepts_postgres_aliases() -> None:
    direct = normalize_database_url("postgresql+psycopg://user:pass@127.0.0.1:5432/receiver")
    assert direct.startswith("postgresql+psycopg://")

    alias = normalize_database_url("postgresql://user:pass@127.0.0.1:5432/receiver")
    assert alias.startswith("postgresql+psycopg://")

    short_alias = normalize_database_url("postgres://user:pass@127.0.0.1:5432/receiver")
    assert short_alias.startswith("postgresql+psycopg://")


def test_normalize_database_url_accepts_sqlite() -> None:
    sqlite_url = "sqlite:///tmp/receiver.sqlite3"
    assert normalize_database_url(sqlite_url) == sqlite_url


def test_normalize_database_url_rejects_mysql() -> None:
    with pytest.raises(ValueError, match="Unsupported DATABASE_URL driver"):
        normalize_database_url("mysql+pymysql://user:pass@127.0.0.1:3306/receiver")


def test_postgres_column_types_are_native() -> None:
    dialect = postgresql.dialect()

    crawl_task_id_type = str(CrawlTask.__table__.c.id.type.compile(dialect=dialect)).upper()
    dispatch_meta_type = str(TaskRun.__table__.c.dispatch_meta_json.type.compile(dialect=dialect)).upper()
    run_status_type = str(TaskRun.__table__.c.status.type.compile(dialect=dialect)).lower()
    artifact_lng_type = str(RunArtifact.__table__.c.longitude.type.compile(dialect=dialect)).upper()
    product_price_type = str(RunArtifactProduct.__table__.c.price.type.compile(dialect=dialect)).upper()
    product_categories_type = str(RunArtifactProduct.__table__.c.categories_uid_json.type.compile(dialect=dialect)).upper()

    assert crawl_task_id_type == "BIGINT"
    assert dispatch_meta_type == "JSONB"
    assert "task_run_status" in run_status_type
    assert artifact_lng_type == "NUMERIC(9, 6)"
    assert product_price_type == "NUMERIC(12, 4)"
    assert product_categories_type == "JSONB"
