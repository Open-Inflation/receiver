from __future__ import annotations

import hashlib
import importlib
import io
import json
import logging
import sys
import tarfile
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import (
    RunArtifact,
    RunArtifactAdministrativeUnit,
    RunArtifactCategory,
    RunArtifactProduct,
    RunArtifactProductCategory,
    RunArtifactProductImage,
    RunArtifactProductMeta,
    RunArtifactProductWholesalePrice,
    TaskRun,
)


LOGGER = logging.getLogger(__name__)


class ArtifactIngestor:
    def __init__(self, *, parser_src_path: Path):
        self._from_json: Any = None
        self._retail_unit_model: Any = None
        self._init_dataclass_support(parser_src_path)

    @property
    def dataclass_enabled(self) -> bool:
        return bool(self._from_json and self._retail_unit_model)

    def ingest_run_output(self, session: Session, *, run: TaskRun) -> dict[str, Any]:
        payload, source, source_error = self._load_payload(run)
        if payload is None:
            return {
                "ok": False,
                "error": source_error or "artifact payload not found",
                "source": None,
            }

        image_url_lookup = self._build_image_url_lookup(run.image_results_json)
        normalized_payload = payload
        dataclass_validated = False
        dataclass_validation_error: str | None = None

        if self.dataclass_enabled:
            try:
                parsed = self._from_json(
                    json.dumps(payload, ensure_ascii=False),
                    self._retail_unit_model,
                )
                if hasattr(parsed, "model_dump"):
                    normalized_payload = parsed.model_dump(mode="json")
                dataclass_validated = True
            except Exception as exc:
                dataclass_validation_error = self._truncate_error(str(exc))
                LOGGER.debug(
                    "Artifact dataclass validation failed for run %s: %s",
                    run.id,
                    dataclass_validation_error,
                )

        artifact = self._persist_payload(
            session,
            run=run,
            payload=normalized_payload,
            source=source,
            image_url_lookup=image_url_lookup,
            dataclass_validated=dataclass_validated,
            dataclass_validation_error=dataclass_validation_error,
        )

        return {
            "ok": True,
            "source": source,
            "artifact_id": artifact.id,
            "category_rows": len(artifact.categories),
            "product_rows": len(artifact.products),
            "dataclass_validated": dataclass_validated,
            "dataclass_validation_error": dataclass_validation_error,
        }

    def _init_dataclass_support(self, parser_src_path: Path) -> None:
        candidates: list[Path] = []

        inferred = parser_src_path.resolve().parents[1] / "dataclass" / "src"
        candidates.append(inferred)

        try:
            current_repo_guess = Path(__file__).resolve().parents[3] / "dataclass" / "src"
            candidates.append(current_repo_guess)
        except Exception:
            pass

        for candidate in candidates:
            if candidate.is_dir() and str(candidate) not in sys.path:
                sys.path.insert(0, str(candidate))

        try:
            module = importlib.import_module("openinflation_dataclass")
        except Exception as exc:
            LOGGER.warning("openinflation_dataclass import failed: %s", exc)
            return

        self._from_json = getattr(module, "from_json", None)
        self._retail_unit_model = getattr(module, "RetailUnit", None)
        if not self.dataclass_enabled:
            LOGGER.warning("openinflation_dataclass integration is partially unavailable")

    def _load_payload(self, run: TaskRun) -> tuple[dict[str, Any] | None, str | None, str | None]:
        if isinstance(run.output_json, str) and run.output_json.strip():
            payload, error = self._load_from_output_json(run.output_json)
            if payload is not None:
                return payload, "output_json", None
            if error:
                LOGGER.debug("Run %s output_json read failed: %s", run.id, error)

        if isinstance(run.output_gz, str) and run.output_gz.strip():
            payload, error = self._load_from_archive_path(Path(run.output_gz.strip()))
            if payload is not None:
                return payload, "output_gz", None
            if error:
                LOGGER.debug("Run %s output_gz read failed: %s", run.id, error)

        if isinstance(run.download_url, str) and run.download_url.strip():
            payload, error = self._load_from_download_url(
                run.download_url.strip(),
                expected_sha256=self._safe_str(run.download_sha256),
            )
            if payload is not None:
                return payload, "download_url", None
            if error:
                LOGGER.debug("Run %s download_url read failed: %s", run.id, error)
                return None, None, error

        return None, None, "Unable to read artifact from output_json/output_gz/download_url"

    def _load_from_output_json(self, output_json: str) -> tuple[dict[str, Any] | None, str | None]:
        token = output_json.strip()
        if not token:
            return None, "output_json is empty"

        path = Path(token)
        if path.is_file():
            try:
                return self._loads_json(path.read_text(encoding="utf-8")), None
            except Exception as exc:
                return None, f"failed to parse JSON file {path}: {exc}"

        # Some clients may send payload itself instead of a path.
        if token.startswith("{"):
            try:
                return self._loads_json(token), None
            except Exception as exc:
                return None, f"failed to parse output_json payload: {exc}"

        return None, f"output_json path does not exist: {token}"

    def _load_from_archive_path(self, archive_path: Path) -> tuple[dict[str, Any] | None, str | None]:
        if not archive_path.is_file():
            return None, f"archive file does not exist: {archive_path}"

        try:
            payload = archive_path.read_bytes()
        except Exception as exc:
            return None, f"failed to read archive file {archive_path}: {exc}"

        return self._extract_json_payload(payload)

    def _load_from_download_url(
        self,
        url: str,
        *,
        expected_sha256: str | None,
    ) -> tuple[dict[str, Any] | None, str | None]:
        try:
            with httpx.Client(timeout=60.0, follow_redirects=True) as client:
                response = client.get(url)
                response.raise_for_status()
        except Exception as exc:
            return None, f"failed to download artifact: {exc}"

        payload = response.content
        if expected_sha256:
            actual_sha256 = hashlib.sha256(payload).hexdigest()
            if actual_sha256 != expected_sha256:
                return None, "downloaded payload checksum mismatch"

        return self._extract_json_payload(payload)

    def _extract_json_payload(self, payload: bytes) -> tuple[dict[str, Any] | None, str | None]:
        try:
            with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as archive:
                prioritized_members = ["meta.json", "output.json", "result.json"]
                for member_name in prioritized_members:
                    member = archive.getmember(member_name) if member_name in archive.getnames() else None
                    if member is None:
                        continue
                    parsed = self._load_json_member(archive, member)
                    if parsed is not None:
                        return parsed, None

                for member in archive.getmembers():
                    if not member.isfile() or not member.name.lower().endswith(".json"):
                        continue
                    parsed = self._load_json_member(archive, member)
                    if parsed is not None:
                        return parsed, None
        except tarfile.ReadError:
            try:
                return self._loads_json(payload.decode("utf-8")), None
            except Exception as exc:
                return None, f"payload is neither tar.gz nor JSON: {exc}"
        except Exception as exc:
            return None, f"failed to extract JSON from archive: {exc}"

        return None, "archive does not contain JSON artifact"

    def _load_json_member(self, archive: tarfile.TarFile, member: tarfile.TarInfo) -> dict[str, Any] | None:
        extracted = archive.extractfile(member)
        if extracted is None:
            return None

        try:
            raw = extracted.read()
            return self._loads_json(raw.decode("utf-8"))
        except Exception:
            return None

    @staticmethod
    def _loads_json(payload: str) -> dict[str, Any]:
        parsed = json.loads(payload)
        if not isinstance(parsed, dict):
            raise ValueError("artifact root is not a JSON object")
        return parsed

    def _persist_payload(
        self,
        session: Session,
        *,
        run: TaskRun,
        payload: dict[str, Any],
        source: str,
        image_url_lookup: dict[str, dict[str, str]],
        dataclass_validated: bool,
        dataclass_validation_error: str | None,
    ) -> RunArtifact:
        existing = session.scalar(select(RunArtifact).where(RunArtifact.run_id == run.id))
        if existing is not None:
            session.delete(existing)
            session.flush()

        schedule_weekdays_open, schedule_weekdays_closed = self._schedule_times(payload.get("schedule_weekdays"))
        schedule_saturday_open, schedule_saturday_closed = self._schedule_times(payload.get("schedule_saturday"))
        schedule_sunday_open, schedule_sunday_closed = self._schedule_times(payload.get("schedule_sunday"))

        artifact = RunArtifact(
            run_id=run.id,
            source=source,
            retail_type=self._safe_str(payload.get("retail_type")),
            code=self._safe_str(payload.get("code")),
            address=self._safe_str(payload.get("address")),
            schedule_weekdays_open_from=schedule_weekdays_open,
            schedule_weekdays_closed_from=schedule_weekdays_closed,
            schedule_saturday_open_from=schedule_saturday_open,
            schedule_saturday_closed_from=schedule_saturday_closed,
            schedule_sunday_open_from=schedule_sunday_open,
            schedule_sunday_closed_from=schedule_sunday_closed,
            temporarily_closed=self._as_bool(payload.get("temporarily_closed")),
            longitude=self._as_float(payload.get("longitude")),
            latitude=self._as_float(payload.get("latitude")),
            dataclass_validated=dataclass_validated,
            dataclass_validation_error=dataclass_validation_error,
            payload_json=payload,
        )

        admin_unit = payload.get("administrative_unit")
        if isinstance(admin_unit, dict):
            artifact.administrative_unit = RunArtifactAdministrativeUnit(
                settlement_type=self._safe_str(admin_unit.get("settlement_type")),
                name=self._safe_str(admin_unit.get("name")),
                alias=self._safe_str(admin_unit.get("alias")),
                country=self._safe_str(admin_unit.get("country")),
                region=self._safe_str(admin_unit.get("region")),
                longitude=self._as_float(admin_unit.get("longitude")),
                latitude=self._as_float(admin_unit.get("latitude")),
            )

        categories = payload.get("categories")
        if isinstance(categories, list):
            self._append_categories(
                artifact,
                categories,
                parent_uid=None,
                depth=0,
            )

        products = payload.get("products")
        if isinstance(products, list):
            for sort_order, product_payload in enumerate(products):
                if not isinstance(product_payload, dict):
                    continue
                product = self._build_product(
                    product_payload,
                    sort_order=sort_order,
                    image_url_lookup=image_url_lookup,
                )
                artifact.products.append(product)

        session.add(artifact)
        session.commit()
        session.refresh(artifact)
        return artifact

    def _append_categories(
        self,
        artifact: RunArtifact,
        raw_categories: list[Any],
        *,
        parent_uid: str | None,
        depth: int,
    ) -> None:
        for sort_order, raw_category in enumerate(raw_categories):
            if not isinstance(raw_category, dict):
                continue

            current_uid = self._safe_str(raw_category.get("uid"))
            category = RunArtifactCategory(
                uid=current_uid,
                parent_uid=parent_uid,
                alias=self._safe_str(raw_category.get("alias")),
                title=self._safe_str(raw_category.get("title")),
                adult=self._as_bool(raw_category.get("adult")),
                icon=self._safe_str(raw_category.get("icon")),
                banner=self._safe_str(raw_category.get("banner")),
                depth=depth,
                sort_order=sort_order,
            )
            artifact.categories.append(category)

            children = raw_category.get("children")
            if isinstance(children, list) and children:
                self._append_categories(
                    artifact,
                    children,
                    parent_uid=current_uid,
                    depth=depth + 1,
                )

    def _build_product(
        self,
        payload: dict[str, Any],
        *,
        sort_order: int,
        image_url_lookup: dict[str, dict[str, str]],
    ) -> RunArtifactProduct:
        categories_uid = self._normalize_string_list(payload.get("categories_uid"))
        main_image_path = self._safe_str(payload.get("main_image"))
        main_image_value = self._resolve_image_url(main_image_path, image_url_lookup) or main_image_path

        product = RunArtifactProduct(
            sku=self._safe_str(payload.get("sku")),
            plu=self._safe_str(payload.get("plu")),
            source_page_url=self._safe_str(payload.get("source_page_url")),
            title=self._safe_str(payload.get("title")),
            description=self._safe_str(payload.get("description")),
            adult=self._as_bool(payload.get("adult")),
            is_new=self._as_bool(payload.get("new")),
            promo=self._as_bool(payload.get("promo")),
            season=self._as_bool(payload.get("season")),
            hit=self._as_bool(payload.get("hit")),
            data_matrix=self._as_bool(payload.get("data_matrix")),
            brand=self._safe_str(payload.get("brand")),
            producer_name=self._safe_str(payload.get("producer_name")),
            producer_country=self._safe_str(payload.get("producer_country")),
            composition=self._safe_str(payload.get("composition")),
            expiration_date_in_days=self._as_int(payload.get("expiration_date_in_days")),
            rating=self._as_float(payload.get("rating")),
            reviews_count=self._as_int(payload.get("reviews_count")),
            price=self._as_float(payload.get("price")),
            discount_price=self._as_float(payload.get("discount_price")),
            loyal_price=self._as_float(payload.get("loyal_price")),
            price_unit=self._safe_str(payload.get("price_unit")),
            unit=self._safe_str(payload.get("unit")),
            available_count=self._as_float(payload.get("available_count")),
            package_quantity=self._as_float(payload.get("package_quantity")),
            package_unit=self._safe_str(payload.get("package_unit")),
            categories_uid_json=categories_uid or None,
            main_image=main_image_value,
            sort_order=sort_order,
        )

        if main_image_path is not None:
            product.images.append(
                RunArtifactProductImage(
                    url=main_image_value,
                    is_main=True,
                    sort_order=0,
                )
            )

        gallery_images = payload.get("images")
        if isinstance(gallery_images, list):
            for index, raw_image in enumerate(gallery_images, start=1):
                image_url = self._safe_str(raw_image)
                if image_url is None:
                    continue
                storage_url = self._resolve_image_url(image_url, image_url_lookup) or image_url
                product.images.append(
                    RunArtifactProductImage(
                        url=storage_url,
                        is_main=False,
                        sort_order=index,
                    )
                )

        meta_data = payload.get("meta_data")
        if isinstance(meta_data, list):
            for index, raw_meta in enumerate(meta_data):
                if not isinstance(raw_meta, dict):
                    continue
                value = raw_meta.get("value")
                product.metadata_entries.append(
                    RunArtifactProductMeta(
                        name=self._safe_str(raw_meta.get("name")),
                        alias=self._safe_str(raw_meta.get("alias")),
                        value_type=self._value_type(value),
                        value_text=self._value_text(value),
                        sort_order=index,
                    )
                )

        wholesale_prices = payload.get("wholesale_price")
        if isinstance(wholesale_prices, list):
            for index, raw_price in enumerate(wholesale_prices):
                if not isinstance(raw_price, dict):
                    continue
                product.wholesale_prices.append(
                    RunArtifactProductWholesalePrice(
                        from_items=self._as_float(raw_price.get("from_items")),
                        price=self._as_float(raw_price.get("price")),
                        sort_order=index,
                    )
                )

        for index, category_uid in enumerate(categories_uid):
            product.category_links.append(
                RunArtifactProductCategory(
                    category_uid=category_uid,
                    sort_order=index,
                )
            )

        return product

    @staticmethod
    def _schedule_times(value: Any) -> tuple[str | None, str | None]:
        if not isinstance(value, dict):
            return None, None
        return ArtifactIngestor._safe_str(value.get("open_from")), ArtifactIngestor._safe_str(
            value.get("closed_from")
        )

    @staticmethod
    def _normalize_string_list(value: Any) -> list[str]:
        if not isinstance(value, list):
            return []
        result: list[str] = []
        for item in value:
            token = ArtifactIngestor._safe_str(item)
            if token is not None:
                result.append(token)
        return result

    @staticmethod
    def _safe_str(value: Any) -> str | None:
        if value is None:
            return None
        token = str(value).strip()
        return token or None

    @staticmethod
    def _as_bool(value: Any) -> bool | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            token = value.strip().lower()
            if token in {"1", "true", "yes", "y", "on"}:
                return True
            if token in {"0", "false", "no", "n", "off"}:
                return False
        return None

    @staticmethod
    def _as_int(value: Any) -> int | None:
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value) if value.is_integer() else None
        if isinstance(value, str):
            token = value.strip()
            if not token:
                return None
            try:
                return int(token)
            except ValueError:
                return None
        return None

    @staticmethod
    def _as_float(value: Any) -> float | None:
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            token = value.strip().replace(",", ".")
            if not token:
                return None
            try:
                return float(token)
            except ValueError:
                return None
        return None

    @staticmethod
    def _value_type(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, int):
            return "int"
        if isinstance(value, float):
            return "float"
        if isinstance(value, str):
            return "str"
        return type(value).__name__

    @staticmethod
    def _value_text(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        return str(value)

    @staticmethod
    def _truncate_error(error_text: str, *, limit: int = 4000) -> str:
        if len(error_text) <= limit:
            return error_text
        return f"{error_text[:limit]}... [truncated]"

    @staticmethod
    def _normalize_image_key(path: str) -> str:
        normalized = path.strip().replace("\\", "/")
        while normalized.startswith("./"):
            normalized = normalized[2:]
        return normalized.lstrip("/")

    def _build_image_url_lookup(self, image_results: Any) -> dict[str, dict[str, str]]:
        by_path: dict[str, str] = {}
        by_path_lower: dict[str, str] = {}
        basename_values: dict[str, str] = {}
        basename_counts: dict[str, int] = {}

        if not isinstance(image_results, list):
            return {
                "by_path": by_path,
                "by_path_lower": by_path_lower,
                "by_basename_unique": {},
            }

        for item in image_results:
            if not isinstance(item, dict):
                continue
            uploaded_url = self._safe_str(item.get("uploaded_url"))
            if uploaded_url is None:
                continue

            source_path = self._safe_str(item.get("source_path")) or self._safe_str(item.get("archive_path"))
            if source_path:
                key = self._normalize_image_key(source_path)
                by_path[key] = uploaded_url
                by_path_lower[key.lower()] = uploaded_url

            filename = self._safe_str(item.get("filename"))
            if filename:
                base_key = Path(filename).name.lower()
                basename_counts[base_key] = basename_counts.get(base_key, 0) + 1
                if base_key not in basename_values:
                    basename_values[base_key] = uploaded_url

        by_basename_unique = {
            key: value
            for key, value in basename_values.items()
            if basename_counts.get(key, 0) == 1
        }
        return {
            "by_path": by_path,
            "by_path_lower": by_path_lower,
            "by_basename_unique": by_basename_unique,
        }

    def _resolve_image_url(self, source_path: str | None, lookup: dict[str, dict[str, str]]) -> str | None:
        if source_path is None:
            return None
        key = self._normalize_image_key(source_path)

        direct = lookup.get("by_path", {}).get(key)
        if direct:
            return direct

        lowered = lookup.get("by_path_lower", {}).get(key.lower())
        if lowered:
            return lowered

        basename_key = Path(key).name.lower()
        return lookup.get("by_basename_unique", {}).get(basename_key)
