from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class TaskCreate(BaseModel):
    city: str = Field(min_length=1, max_length=120)
    store: str = Field(min_length=1, max_length=120)
    frequency_hours: int = Field(ge=1, le=24 * 365)
    last_crawl_at: datetime | None = None
    parser_name: str = Field(default="fixprice", min_length=1, max_length=64)
    is_active: bool = True


class TaskUpdate(BaseModel):
    city: str | None = Field(default=None, min_length=1, max_length=120)
    store: str | None = Field(default=None, min_length=1, max_length=120)
    frequency_hours: int | None = Field(default=None, ge=1, le=24 * 365)
    last_crawl_at: datetime | None = None
    parser_name: str | None = Field(default=None, min_length=1, max_length=64)
    is_active: bool | None = None


class TaskOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    city: str
    store: str
    frequency_hours: int
    last_crawl_at: datetime | None
    parser_name: str
    is_active: bool
    lease_owner_id: str | None
    lease_until: datetime | None
    created_at: datetime
    updated_at: datetime


class RegisterOrchestratorIn(BaseModel):
    name: str = Field(min_length=1, max_length=120)


class RegisterOrchestratorOut(BaseModel):
    orchestrator_id: str
    token: str


class HeartbeatOut(BaseModel):
    orchestrator_id: str
    last_heartbeat_at: datetime


class AssignmentOut(BaseModel):
    run_id: str
    task: TaskOut


class NextTaskOut(BaseModel):
    assignment: AssignmentOut | None


class IncomingImage(BaseModel):
    filename: str | None = Field(default=None, max_length=255)
    content_base64: str | None = None


class SubmitResultIn(BaseModel):
    run_id: str = Field(min_length=1, max_length=32)
    status: Literal["success", "error"]
    payload: dict[str, Any] | None = None
    images: list[IncomingImage] = Field(default_factory=list)
    upload_images_from_archive: bool = False
    output_json: str | None = None
    output_gz: str | None = None
    download_url: str | None = None
    download_sha256: str | None = None
    download_expires_at: datetime | None = None
    error_message: str | None = None


class SubmitResultOut(BaseModel):
    run_id: str
    status: Literal["success", "error"]
    processed_images: int
    image_results: list[dict[str, Any]]
    output_json: str | None
    output_gz: str | None
    download_url: str | None
    download_sha256: str | None
    download_expires_at: datetime | None
    parser_payload: dict[str, Any] | None


class TaskRunOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    task_id: int
    orchestrator_id: str
    status: str
    assigned_at: datetime
    finished_at: datetime | None
    payload_json: dict[str, Any] | None
    parser_payload_json: dict[str, Any] | None
    image_results_json: list[dict[str, Any]] | None
    output_json: str | None
    output_gz: str | None
    download_url: str | None
    download_sha256: str | None
    download_expires_at: datetime | None
    error_message: str | None
