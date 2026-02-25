from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, Index, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .database import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Orchestrator(Base):
    __tablename__ = "orchestrators"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)
    name: Mapped[str] = mapped_column(String(120), unique=True, nullable=False, index=True)
    token: Mapped[str] = mapped_column(String(128), unique=True, nullable=False, index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
        onupdate=utcnow,
    )
    last_heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)

    runs: Mapped[list["TaskRun"]] = relationship(back_populates="orchestrator")


class CrawlTask(Base):
    __tablename__ = "crawl_tasks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    city: Mapped[str] = mapped_column(String(120), nullable=False)
    store: Mapped[str] = mapped_column(String(120), nullable=False)
    frequency_hours: Mapped[int] = mapped_column(Integer, nullable=False)
    last_crawl_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    parser_name: Mapped[str] = mapped_column(String(64), nullable=False, default="fixprice")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    lease_owner_id: Mapped[str | None] = mapped_column(
        String(32),
        ForeignKey("orchestrators.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utcnow,
        onupdate=utcnow,
    )

    runs: Mapped[list["TaskRun"]] = relationship(back_populates="task")

    __table_args__ = (
        Index("ix_crawl_tasks_due_scan", "is_active", "lease_until", "last_crawl_at"),
    )


class TaskRun(Base):
    __tablename__ = "task_runs"

    id: Mapped[str] = mapped_column(String(32), primary_key=True)

    task_id: Mapped[int] = mapped_column(ForeignKey("crawl_tasks.id", ondelete="CASCADE"), nullable=False, index=True)
    orchestrator_id: Mapped[str] = mapped_column(
        ForeignKey("orchestrators.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    status: Mapped[str] = mapped_column(String(16), nullable=False, default="assigned")
    assigned_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    payload_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    parser_payload_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    image_results_json: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON, nullable=True)
    output_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    output_gz: Mapped[str | None] = mapped_column(Text, nullable=True)
    download_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    download_sha256: Mapped[str | None] = mapped_column(String(128), nullable=True)
    download_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    task: Mapped[CrawlTask] = relationship(back_populates="runs")
    orchestrator: Mapped[Orchestrator] = relationship(back_populates="runs")
