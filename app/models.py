from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import BigInteger, Boolean, DateTime, Enum, Float, ForeignKey, Index, Integer, JSON, Numeric, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .database import Base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


RUN_STATUS_ENUM = Enum(
    "assigned",
    "success",
    "error",
    name="task_run_status",
    native_enum=True,
    create_constraint=True,
    validate_strings=True,
)


def _bigint_sqlite() -> BigInteger:
    return BigInteger().with_variant(Integer(), "sqlite")


def _json_postgres() -> JSON:
    return JSON().with_variant(JSONB(), "postgresql")


def _money_numeric() -> Numeric:
    return Numeric(12, 4).with_variant(Float(), "sqlite")


def _coord_numeric() -> Numeric:
    return Numeric(9, 6).with_variant(Float(), "sqlite")


def _quantity_numeric() -> Numeric:
    return Numeric(14, 4).with_variant(Float(), "sqlite")


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

    id: Mapped[int] = mapped_column(_bigint_sqlite(), primary_key=True, autoincrement=True)

    city: Mapped[str] = mapped_column(String(120), nullable=False)
    store: Mapped[str] = mapped_column(String(120), nullable=False)
    frequency_hours: Mapped[int] = mapped_column(Integer, nullable=False)
    last_crawl_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    parser_name: Mapped[str] = mapped_column(String(64), nullable=False, default="fixprice")
    include_images: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, index=True)

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

    id: Mapped[str] = mapped_column(String(64), primary_key=True)

    task_id: Mapped[int] = mapped_column(
        _bigint_sqlite(),
        ForeignKey("crawl_tasks.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    orchestrator_id: Mapped[str] = mapped_column(
        ForeignKey("orchestrators.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    status: Mapped[str] = mapped_column(RUN_STATUS_ENUM, nullable=False, default="assigned")
    assigned_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    dispatch_meta_json: Mapped[dict[str, Any] | None] = mapped_column(_json_postgres(), nullable=True)
    processed_images: Mapped[int] = mapped_column(_bigint_sqlite(), nullable=False, default=0)
    converter_elapsed_sec: Mapped[int] = mapped_column(_bigint_sqlite(), nullable=False, default=0)
    finish: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)

    task: Mapped[CrawlTask] = relationship(back_populates="runs")
    orchestrator: Mapped[Orchestrator] = relationship(back_populates="runs")
    parsed_artifact: Mapped["RunArtifact | None"] = relationship(
        back_populates="run",
        uselist=False,
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_task_runs_status_task_id", "status", "task_id"),
        Index("ix_task_runs_assigned_at", "assigned_at"),
        Index("ix_task_runs_status_orchestrator_assigned_at", "status", "orchestrator_id", "assigned_at"),
    )


class RunArtifact(Base):
    __tablename__ = "run_artifacts"

    id: Mapped[int] = mapped_column(_bigint_sqlite(), primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(
        ForeignKey("task_runs.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )
    source: Mapped[str] = mapped_column(String(255), nullable=False)
    parser_name: Mapped[str] = mapped_column(String(64), nullable=False)

    retail_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    code: Mapped[str | None] = mapped_column(String(128), nullable=True)
    address: Mapped[str | None] = mapped_column(Text, nullable=True)

    schedule_weekdays_open_from: Mapped[str | None] = mapped_column(String(16), nullable=True)
    schedule_weekdays_closed_from: Mapped[str | None] = mapped_column(String(16), nullable=True)
    schedule_saturday_open_from: Mapped[str | None] = mapped_column(String(16), nullable=True)
    schedule_saturday_closed_from: Mapped[str | None] = mapped_column(String(16), nullable=True)
    schedule_sunday_open_from: Mapped[str | None] = mapped_column(String(16), nullable=True)
    schedule_sunday_closed_from: Mapped[str | None] = mapped_column(String(16), nullable=True)

    temporarily_closed: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    longitude: Mapped[float | None] = mapped_column(_coord_numeric(), nullable=True)
    latitude: Mapped[float | None] = mapped_column(_coord_numeric(), nullable=True)

    dataclass_validated: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    dataclass_validation_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, default=utcnow)

    run: Mapped[TaskRun] = relationship(back_populates="parsed_artifact")
    administrative_unit: Mapped["RunArtifactAdministrativeUnit | None"] = relationship(
        back_populates="artifact",
        uselist=False,
        cascade="all, delete-orphan",
    )
    categories: Mapped[list["RunArtifactCategory"]] = relationship(
        back_populates="artifact",
        cascade="all, delete-orphan",
    )
    products: Mapped[list["RunArtifactProduct"]] = relationship(
        back_populates="artifact",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_run_artifacts_parser_ingested_id", "parser_name", "ingested_at", "id"),
    )


class RunArtifactAdministrativeUnit(Base):
    __tablename__ = "run_artifact_administrative_units"

    id: Mapped[int] = mapped_column(_bigint_sqlite(), primary_key=True, autoincrement=True)
    artifact_id: Mapped[int] = mapped_column(
        _bigint_sqlite(),
        ForeignKey("run_artifacts.id", ondelete="CASCADE"),
        nullable=False,
        unique=True,
        index=True,
    )

    settlement_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    alias: Mapped[str | None] = mapped_column(String(255), nullable=True)
    country: Mapped[str | None] = mapped_column(String(32), nullable=True)
    region: Mapped[str | None] = mapped_column(String(255), nullable=True)
    longitude: Mapped[float | None] = mapped_column(_coord_numeric(), nullable=True)
    latitude: Mapped[float | None] = mapped_column(_coord_numeric(), nullable=True)

    artifact: Mapped[RunArtifact] = relationship(back_populates="administrative_unit")


class RunArtifactCategory(Base):
    __tablename__ = "run_artifact_categories"

    id: Mapped[int] = mapped_column(_bigint_sqlite(), primary_key=True, autoincrement=True)
    artifact_id: Mapped[int] = mapped_column(
        _bigint_sqlite(),
        ForeignKey("run_artifacts.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    uid: Mapped[str | None] = mapped_column(String(128), nullable=True)
    parent_uid: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    alias: Mapped[str | None] = mapped_column(String(255), nullable=True)
    title: Mapped[str | None] = mapped_column(String(255), nullable=True)
    adult: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    icon: Mapped[str | None] = mapped_column(Text, nullable=True)
    banner: Mapped[str | None] = mapped_column(Text, nullable=True)
    depth: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    artifact: Mapped[RunArtifact] = relationship(back_populates="categories")

    __table_args__ = (
        Index("ix_run_artifact_categories_artifact_uid", "artifact_id", "uid"),
    )


class RunArtifactProduct(Base):
    __tablename__ = "run_artifact_products"

    id: Mapped[int] = mapped_column(_bigint_sqlite(), primary_key=True, autoincrement=True)
    artifact_id: Mapped[int] = mapped_column(
        _bigint_sqlite(),
        ForeignKey("run_artifacts.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    sku: Mapped[str | None] = mapped_column(String(128), nullable=True)
    plu: Mapped[str | None] = mapped_column(String(128), nullable=True)
    source_page_url: Mapped[str | None] = mapped_column(Text, nullable=True)
    title: Mapped[str | None] = mapped_column(Text, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)

    adult: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    is_new: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    promo: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    season: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    hit: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    data_matrix: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    brand: Mapped[str | None] = mapped_column(String(255), nullable=True)
    producer_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    producer_country: Mapped[str | None] = mapped_column(String(32), nullable=True)
    composition: Mapped[str | None] = mapped_column(Text, nullable=True)

    expiration_date_in_days: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rating: Mapped[float | None] = mapped_column(Numeric(4, 2).with_variant(Float(), "sqlite"), nullable=True)
    reviews_count: Mapped[int | None] = mapped_column(Integer, nullable=True)

    price: Mapped[float | None] = mapped_column(_money_numeric(), nullable=True)
    discount_price: Mapped[float | None] = mapped_column(_money_numeric(), nullable=True)
    loyal_price: Mapped[float | None] = mapped_column(_money_numeric(), nullable=True)
    price_unit: Mapped[str | None] = mapped_column(String(32), nullable=True)

    unit: Mapped[str | None] = mapped_column(String(32), nullable=True)
    available_count: Mapped[float | None] = mapped_column(_quantity_numeric(), nullable=True)
    package_quantity: Mapped[float | None] = mapped_column(_quantity_numeric(), nullable=True)
    package_unit: Mapped[str | None] = mapped_column(String(32), nullable=True)

    categories_uid_json: Mapped[list[str] | None] = mapped_column(_json_postgres(), nullable=True)
    main_image: Mapped[str | None] = mapped_column(Text, nullable=True)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    artifact: Mapped[RunArtifact] = relationship(back_populates="products")
    metadata_entries: Mapped[list["RunArtifactProductMeta"]] = relationship(
        back_populates="product",
        cascade="all, delete-orphan",
    )
    wholesale_prices: Mapped[list["RunArtifactProductWholesalePrice"]] = relationship(
        back_populates="product",
        cascade="all, delete-orphan",
    )
    images: Mapped[list["RunArtifactProductImage"]] = relationship(
        back_populates="product",
        cascade="all, delete-orphan",
    )
    category_links: Mapped[list["RunArtifactProductCategory"]] = relationship(
        back_populates="product",
        cascade="all, delete-orphan",
    )

    __table_args__ = (
        Index("ix_run_artifact_products_artifact_sku", "artifact_id", "sku"),
        Index("ix_run_artifact_products_artifact_id_id", "artifact_id", "id"),
    )


class RunArtifactProductMeta(Base):
    __tablename__ = "run_artifact_product_meta"

    id: Mapped[int] = mapped_column(_bigint_sqlite(), primary_key=True, autoincrement=True)
    product_id: Mapped[int] = mapped_column(
        _bigint_sqlite(),
        ForeignKey("run_artifact_products.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    alias: Mapped[str | None] = mapped_column(String(255), nullable=True)
    value_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    value_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    product: Mapped[RunArtifactProduct] = relationship(back_populates="metadata_entries")


class RunArtifactProductWholesalePrice(Base):
    __tablename__ = "run_artifact_product_wholesale_prices"

    id: Mapped[int] = mapped_column(_bigint_sqlite(), primary_key=True, autoincrement=True)
    product_id: Mapped[int] = mapped_column(
        _bigint_sqlite(),
        ForeignKey("run_artifact_products.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    from_items: Mapped[float | None] = mapped_column(_quantity_numeric(), nullable=True)
    price: Mapped[float | None] = mapped_column(_money_numeric(), nullable=True)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    product: Mapped[RunArtifactProduct] = relationship(back_populates="wholesale_prices")


class RunArtifactProductImage(Base):
    __tablename__ = "run_artifact_product_images"

    id: Mapped[int] = mapped_column(_bigint_sqlite(), primary_key=True, autoincrement=True)
    product_id: Mapped[int] = mapped_column(
        _bigint_sqlite(),
        ForeignKey("run_artifact_products.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    url: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_main: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    product: Mapped[RunArtifactProduct] = relationship(back_populates="images")


class RunArtifactProductCategory(Base):
    __tablename__ = "run_artifact_product_categories"

    id: Mapped[int] = mapped_column(_bigint_sqlite(), primary_key=True, autoincrement=True)
    product_id: Mapped[int] = mapped_column(
        _bigint_sqlite(),
        ForeignKey("run_artifact_products.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    category_uid: Mapped[str | None] = mapped_column(String(128), nullable=True)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    product: Mapped[RunArtifactProduct] = relationship(back_populates="category_links")
