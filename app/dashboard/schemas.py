from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class TaskCreateIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    city: str = Field(min_length=1, max_length=120)
    store: str = Field(min_length=1, max_length=120)
    frequency_hours: int = Field(ge=1, le=24 * 365)
    parser_name: str = Field(default="fixprice", min_length=1, max_length=64)
    include_images: bool | None = None
    use_product_info: bool | None = None
    is_active: bool = True


class TaskUpdateIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    city: str | None = Field(default=None, min_length=1, max_length=120)
    store: str | None = Field(default=None, min_length=1, max_length=120)
    frequency_hours: int | None = Field(default=None, ge=1, le=24 * 365)
    parser_name: str | None = Field(default=None, min_length=1, max_length=64)
    include_images: bool | None = None
    use_product_info: bool | None = None
    is_active: bool | None = None


class StoreDirectorySyncIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    parser_name: str = Field(min_length=1, max_length=64)
    country_id: int | None = None
    city_id: int | str | None = None
    api_timeout_ms: float | None = None
    strict_validation: bool | None = None
