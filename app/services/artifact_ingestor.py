from __future__ import annotations

from dataclasses import dataclass
import hashlib
import importlib
import io
import json
import logging
import sys
import tarfile
import time
from pathlib import Path
from typing import Any

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from ..models import (
    CrawlTask,
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


@dataclass(slots=True)
class _PreparedProduct:
    product: RunArtifactProduct
    images: list[RunArtifactProductImage]
    metadata_entries: list[RunArtifactProductMeta]
    wholesale_prices: list[RunArtifactProductWholesalePrice]
    category_links: list[RunArtifactProductCategory]


class ArtifactIngestor:
    def __init__(
        self,
        *,
        parser_src_path: Path,
        download_max_bytes: int = 256 * 1024 * 1024,
        json_member_max_bytes: int = 16 * 1024 * 1024,
        products_per_txn: int = 200,
        categories_per_txn: int = 1000,
        relations_per_txn: int = 2000,
    ):
        self._from_json: Any = None
        self._retail_unit_model: Any = None
        self._download_max_bytes = max(1, int(download_max_bytes))
        self._json_member_max_bytes = max(1, int(json_member_max_bytes))
        self._products_per_txn = max(1, int(products_per_txn))
        self._categories_per_txn = max(1, int(categories_per_txn))
        self._relations_per_txn = max(1, int(relations_per_txn))
        self._init_dataclass_support(parser_src_path)

    @property
    def dataclass_enabled(self) -> bool:
        return bool(self._from_json and self._retail_unit_model)

    def ingest_run_output(
        self,
        session: Session,
        *,
        run: TaskRun,
        output_json: str | None = None,
        output_gz: str | None = None,
        download_url: str | None = None,
        download_sha256: str | None = None,
        image_results: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        LOGGER.info(
            "Artifact ingest started: run_id=%s output_json=%s output_gz=%s download_url=%s",
            run.id,
            bool(output_json),
            bool(output_gz),
            bool(download_url),
        )
        payload, source, source_error = self._load_payload(
            run_id=run.id,
            output_json=output_json,
            output_gz=output_gz,
            download_url=download_url,
            download_sha256=download_sha256,
        )
        if payload is None:
            LOGGER.warning(
                "Artifact ingest failed: run_id=%s error=%s",
                run.id,
                source_error or "artifact payload not found",
            )
            return {
                "ok": False,
                "error": source_error or "artifact payload not found",
                "source": None,
            }

        image_url_lookup = self._build_image_url_lookup(image_results)
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
        product_rows = len(artifact.products)
        category_rows = len(artifact.categories)
        run.artifact_source = source
        run.artifact_products_count = int(product_rows)
        run.artifact_categories_count = int(category_rows)
        run.artifact_dataclass_validated = bool(dataclass_validated)
        run.artifact_dataclass_validation_error = dataclass_validation_error
        run.artifact_ingested_at = artifact.ingested_at
        session.commit()

        result = {
            "ok": True,
            "source": source,
            "artifact_id": artifact.id,
            "category_rows": category_rows,
            "product_rows": product_rows,
            "dataclass_validated": dataclass_validated,
            "dataclass_validation_error": dataclass_validation_error,
        }
        LOGGER.info(
            "Artifact ingest finished: run_id=%s source=%s artifact_id=%s products=%s categories=%s dataclass_validated=%s",
            run.id,
            source,
            artifact.id,
            product_rows,
            category_rows,
            dataclass_validated,
        )
        return result

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

    def _load_payload(
        self,
        *,
        run_id: str,
        output_json: str | None,
        output_gz: str | None,
        download_url: str | None,
        download_sha256: str | None,
    ) -> tuple[dict[str, Any] | None, str | None, str | None]:
        if isinstance(output_json, str) and output_json.strip():
            payload, error = self._load_from_output_json(output_json)
            if payload is not None:
                return payload, "output_json", None
            if error:
                LOGGER.debug("Run %s output_json read failed: %s", run_id, error)

        if isinstance(output_gz, str) and output_gz.strip():
            payload, error = self._load_from_archive_path(Path(output_gz.strip()))
            if payload is not None:
                return payload, "output_gz", None
            if error:
                LOGGER.debug("Run %s output_gz read failed: %s", run_id, error)

        if isinstance(download_url, str) and download_url.strip():
            payload, error = self._load_from_download_url(
                download_url.strip(),
                expected_sha256=self._safe_str(download_sha256),
            )
            if payload is not None:
                return payload, "download_url", None
            if error:
                LOGGER.debug("Run %s download_url read failed: %s", run_id, error)
                return None, None, error

        return None, None, "Unable to read artifact from output_json/output_gz/download_url"

    def _load_from_output_json(self, output_json: str) -> tuple[dict[str, Any] | None, str | None]:
        token = output_json.strip()
        if not token:
            return None, "output_json is empty"

        path = Path(token)
        if path.is_file():
            try:
                file_size = path.stat().st_size
            except OSError:
                file_size = None
            if file_size is not None and file_size > self._json_member_max_bytes:
                return (
                    None,
                    (
                        f"output_json file is too large ({file_size} bytes), "
                        f"limit is {self._json_member_max_bytes}"
                    ),
                )
            try:
                return self._loads_json(path.read_text(encoding="utf-8")), None
            except Exception as exc:
                return None, f"failed to parse JSON file {path}: {exc}"

        # Some clients may send payload itself instead of a path.
        if token.startswith("{"):
            if len(token.encode("utf-8")) > self._json_member_max_bytes:
                return (
                    None,
                    (
                        "output_json payload is too large "
                        f"(limit {self._json_member_max_bytes} bytes)"
                    ),
                )
            try:
                return self._loads_json(token), None
            except Exception as exc:
                return None, f"failed to parse output_json payload: {exc}"

        return None, f"output_json path does not exist: {token}"

    def _load_from_archive_path(self, archive_path: Path) -> tuple[dict[str, Any] | None, str | None]:
        if not archive_path.is_file():
            return None, f"archive file does not exist: {archive_path}"

        try:
            with tarfile.open(archive_path, mode="r:gz") as archive:
                return self._extract_json_from_tar(archive)
        except tarfile.ReadError:
            try:
                file_size = archive_path.stat().st_size
                if file_size > self._json_member_max_bytes:
                    return (
                        None,
                        (
                            f"archive payload is too large ({file_size} bytes), "
                            f"limit is {self._json_member_max_bytes}"
                        ),
                    )
            except OSError:
                pass
            try:
                return self._loads_json(archive_path.read_text(encoding="utf-8")), None
            except Exception as exc:
                return None, f"payload is neither tar.gz nor JSON: {exc}"
        except Exception as exc:
            return None, f"failed to read archive file {archive_path}: {exc}"

    def _load_from_download_url(
        self,
        url: str,
        *,
        expected_sha256: str | None,
    ) -> tuple[dict[str, Any] | None, str | None]:
        try:
            with httpx.Client(timeout=60.0, follow_redirects=True) as client:
                with client.stream("GET", url) as response:
                    response.raise_for_status()
                    payload_parts: list[bytes] = []
                    payload_size = 0
                    sha256 = hashlib.sha256()
                    for chunk in response.iter_bytes():
                        if not chunk:
                            continue
                        payload_size += len(chunk)
                        if payload_size > self._download_max_bytes:
                            return (
                                None,
                                (
                                    f"downloaded payload exceeds limit "
                                    f"{self._download_max_bytes} bytes"
                                ),
                            )
                        payload_parts.append(chunk)
                        sha256.update(chunk)
        except Exception as exc:
            return None, f"failed to download artifact: {exc}"

        payload = b"".join(payload_parts)
        if expected_sha256:
            actual_sha256 = sha256.hexdigest()
            if actual_sha256 != expected_sha256:
                return None, "downloaded payload checksum mismatch"

        return self._extract_json_payload(payload)

    def _extract_json_payload(self, payload: bytes) -> tuple[dict[str, Any] | None, str | None]:
        try:
            with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as archive:
                return self._extract_json_from_tar(archive)
        except tarfile.ReadError:
            if len(payload) > self._json_member_max_bytes:
                return (
                    None,
                    (
                        "JSON payload is too large "
                        f"(limit {self._json_member_max_bytes} bytes)"
                    ),
                )
            try:
                return self._loads_json(payload.decode("utf-8")), None
            except Exception as exc:
                return None, f"payload is neither tar.gz nor JSON: {exc}"
        except Exception as exc:
            return None, f"failed to extract JSON from archive: {exc}"

    def _extract_json_from_tar(self, archive: tarfile.TarFile) -> tuple[dict[str, Any] | None, str | None]:
        prioritized_members = {"meta.json": 0, "output.json": 1, "result.json": 2}
        best_payload: dict[str, Any] | None = None
        best_priority = 10
        first_error: str | None = None

        for member in archive:
            if not member.isfile() or not member.name.lower().endswith(".json"):
                continue
            parsed, error = self._load_json_member(archive, member)
            if parsed is None:
                if first_error is None and error:
                    first_error = error
                continue

            member_name = Path(member.name).name.lower()
            priority = prioritized_members.get(member_name, 3)
            if priority < best_priority:
                best_payload = parsed
                best_priority = priority
                if priority == 0:
                    break

        if best_payload is not None:
            return best_payload, None
        if first_error is not None:
            return None, first_error
        return None, "archive does not contain JSON artifact"

    def _load_json_member(
        self,
        archive: tarfile.TarFile,
        member: tarfile.TarInfo,
    ) -> tuple[dict[str, Any] | None, str | None]:
        if member.size > self._json_member_max_bytes:
            return (
                None,
                (
                    f"JSON member {member.name} is too large ({member.size} bytes), "
                    f"limit is {self._json_member_max_bytes}"
                ),
            )
        extracted = archive.extractfile(member)
        if extracted is None:
            return None, f"failed to read archive member: {member.name}"

        try:
            raw = extracted.read(self._json_member_max_bytes + 1)
            if len(raw) > self._json_member_max_bytes:
                return (
                    None,
                    (
                        f"JSON member {member.name} payload exceeds limit "
                        f"{self._json_member_max_bytes} bytes"
                    ),
                )
            return self._loads_json(raw.decode("utf-8")), None
        except Exception as exc:
            return None, f"failed to parse JSON member {member.name}: {exc}"

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
        try:
            existing = session.scalar(select(RunArtifact).where(RunArtifact.run_id == run.id))
            if existing is not None:
                session.delete(existing)
                stale_start = time.perf_counter()
                session.commit()
                LOGGER.info(
                    "Artifact ingest stale artifact removed: run_id=%s artifact_id=%s duration_ms=%.2f",
                    run.id,
                    existing.id,
                    (time.perf_counter() - stale_start) * 1000.0,
                )

            parser_name = self._resolve_parser_name(session, run=run)
            schedule_weekdays_open, schedule_weekdays_closed = self._schedule_times(payload.get("schedule_weekdays"))
            schedule_saturday_open, schedule_saturday_closed = self._schedule_times(payload.get("schedule_saturday"))
            schedule_sunday_open, schedule_sunday_closed = self._schedule_times(payload.get("schedule_sunday"))

            artifact = RunArtifact(
                run_id=run.id,
                source=source,
                parser_name=parser_name,
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

            shell_start = time.perf_counter()
            session.add(artifact)
            session.commit()
            session.refresh(artifact)
            LOGGER.info(
                "Artifact ingest shell committed: run_id=%s artifact_id=%s duration_ms=%.2f",
                run.id,
                artifact.id,
                (time.perf_counter() - shell_start) * 1000.0,
            )
            artifact_id = int(artifact.id)

            categories = payload.get("categories")
            if isinstance(categories, list):
                flattened_categories: list[RunArtifactCategory] = []
                self._append_categories(
                    flattened_categories,
                    categories,
                    artifact_id=artifact_id,
                    parent_uid=None,
                    depth=0,
                )
                for chunk in self._iter_chunks(flattened_categories, self._categories_per_txn):
                    if not chunk:
                        continue
                    chunk_start = time.perf_counter()
                    session.add_all(chunk)
                    session.commit()
                    LOGGER.info(
                        "Artifact ingest categories chunk committed: run_id=%s artifact_id=%s rows=%s duration_ms=%.2f",
                        run.id,
                        artifact_id,
                        len(chunk),
                        (time.perf_counter() - chunk_start) * 1000.0,
                    )

            products = payload.get("products")
            if isinstance(products, list):
                prepared_products_chunk: list[_PreparedProduct] = []
                for sort_order, product_payload in enumerate(products):
                    if not isinstance(product_payload, dict):
                        continue
                    prepared_products_chunk.append(
                        self._build_product(
                            product_payload,
                            artifact_id=artifact_id,
                            sort_order=sort_order,
                            image_url_lookup=image_url_lookup,
                        )
                    )
                    if len(prepared_products_chunk) >= self._products_per_txn:
                        self._commit_products_chunk(
                            session,
                            run_id=run.id,
                            artifact_id=artifact_id,
                            prepared_products=prepared_products_chunk,
                        )
                        prepared_products_chunk = []

                if prepared_products_chunk:
                    self._commit_products_chunk(
                        session,
                        run_id=run.id,
                        artifact_id=artifact_id,
                        prepared_products=prepared_products_chunk,
                    )

            session.refresh(artifact)
            return artifact
        except Exception:
            session.rollback()
            self._cleanup_partial_artifact(session, run_id=run.id)
            raise

    def _cleanup_partial_artifact(self, session: Session, *, run_id: str) -> None:
        try:
            stale = session.scalar(select(RunArtifact).where(RunArtifact.run_id == run_id))
            if stale is None:
                return
            stale_id = stale.id
            cleanup_start = time.perf_counter()
            session.delete(stale)
            session.commit()
            LOGGER.warning(
                "Artifact ingest cleanup finished: run_id=%s artifact_id=%s duration_ms=%.2f",
                run_id,
                stale_id,
                (time.perf_counter() - cleanup_start) * 1000.0,
            )
        except Exception:
            session.rollback()
            LOGGER.exception("Artifact ingest cleanup failed: run_id=%s", run_id)

    def _commit_products_chunk(
        self,
        session: Session,
        *,
        run_id: str,
        artifact_id: int,
        prepared_products: list[_PreparedProduct],
    ) -> None:
        if not prepared_products:
            return

        products_chunk_start = time.perf_counter()
        for prepared in prepared_products:
            session.add(prepared.product)
        session.commit()
        LOGGER.info(
            "Artifact ingest products chunk committed: run_id=%s artifact_id=%s rows=%s duration_ms=%.2f",
            run_id,
            artifact_id,
            len(prepared_products),
            (time.perf_counter() - products_chunk_start) * 1000.0,
        )
        self._persist_product_relations(
            session,
            run_id=run_id,
            artifact_id=artifact_id,
            prepared_products=prepared_products,
        )

    def _persist_product_relations(
        self,
        session: Session,
        *,
        run_id: str,
        artifact_id: int,
        prepared_products: list[_PreparedProduct],
    ) -> None:
        image_rows: list[RunArtifactProductImage] = []
        meta_rows: list[RunArtifactProductMeta] = []
        wholesale_rows: list[RunArtifactProductWholesalePrice] = []
        category_rows: list[RunArtifactProductCategory] = []

        for prepared in prepared_products:
            product_id = prepared.product.id
            if product_id is None:
                continue
            for image in prepared.images:
                image.product_id = int(product_id)
                image_rows.append(image)
            for metadata in prepared.metadata_entries:
                metadata.product_id = int(product_id)
                meta_rows.append(metadata)
            for wholesale in prepared.wholesale_prices:
                wholesale.product_id = int(product_id)
                wholesale_rows.append(wholesale)
            for category_link in prepared.category_links:
                category_link.product_id = int(product_id)
                category_rows.append(category_link)

        self._commit_row_chunks(
            session,
            run_id=run_id,
            artifact_id=artifact_id,
            table_name="run_artifact_product_images",
            rows=image_rows,
            chunk_size=self._relations_per_txn,
        )
        self._commit_row_chunks(
            session,
            run_id=run_id,
            artifact_id=artifact_id,
            table_name="run_artifact_product_meta",
            rows=meta_rows,
            chunk_size=self._relations_per_txn,
        )
        self._commit_row_chunks(
            session,
            run_id=run_id,
            artifact_id=artifact_id,
            table_name="run_artifact_product_wholesale_prices",
            rows=wholesale_rows,
            chunk_size=self._relations_per_txn,
        )
        self._commit_row_chunks(
            session,
            run_id=run_id,
            artifact_id=artifact_id,
            table_name="run_artifact_product_categories",
            rows=category_rows,
            chunk_size=self._relations_per_txn,
        )

    def _commit_row_chunks(
        self,
        session: Session,
        *,
        run_id: str,
        artifact_id: int,
        table_name: str,
        rows: list[Any],
        chunk_size: int,
    ) -> None:
        for chunk in self._iter_chunks(rows, chunk_size):
            if not chunk:
                continue
            chunk_start = time.perf_counter()
            session.add_all(chunk)
            session.commit()
            LOGGER.info(
                "Artifact ingest relations chunk committed: run_id=%s artifact_id=%s table=%s rows=%s duration_ms=%.2f",
                run_id,
                artifact_id,
                table_name,
                len(chunk),
                (time.perf_counter() - chunk_start) * 1000.0,
            )

    def _resolve_parser_name(self, session: Session, *, run: TaskRun) -> str:
        dispatch_meta = run.dispatch_meta_json if isinstance(run.dispatch_meta_json, dict) else {}
        request_payload = dispatch_meta.get("request")
        if isinstance(request_payload, dict):
            parser_name = self._safe_str(request_payload.get("parser"))
            if parser_name is not None:
                return parser_name

        task = session.get(CrawlTask, run.task_id)
        task_parser_name = self._safe_str(task.parser_name) if task is not None else None
        return task_parser_name or "unknown"

    def _append_categories(
        self,
        output_rows: list[RunArtifactCategory],
        raw_categories: list[Any],
        *,
        artifact_id: int,
        parent_uid: str | None,
        depth: int,
    ) -> None:
        for sort_order, raw_category in enumerate(raw_categories):
            if not isinstance(raw_category, dict):
                continue

            current_uid = self._safe_str(raw_category.get("uid"))
            category = RunArtifactCategory(
                artifact_id=artifact_id,
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
            output_rows.append(category)

            children = raw_category.get("children")
            if isinstance(children, list) and children:
                self._append_categories(
                    output_rows,
                    children,
                    artifact_id=artifact_id,
                    parent_uid=current_uid,
                    depth=depth + 1,
                )

    def _build_product(
        self,
        payload: dict[str, Any],
        *,
        artifact_id: int,
        sort_order: int,
        image_url_lookup: dict[str, dict[str, str]],
    ) -> _PreparedProduct:
        categories_uid = self._normalize_string_list(payload.get("categories_uid"))
        main_image_path = self._safe_str(payload.get("main_image"))
        main_image_value = self._resolve_image_url(main_image_path, image_url_lookup) or main_image_path

        product = RunArtifactProduct(
            artifact_id=artifact_id,
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
        images: list[RunArtifactProductImage] = []
        metadata_entries: list[RunArtifactProductMeta] = []
        wholesale_prices: list[RunArtifactProductWholesalePrice] = []
        category_links: list[RunArtifactProductCategory] = []

        if main_image_path is not None:
            images.append(
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
                images.append(
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
                metadata_entries.append(
                    RunArtifactProductMeta(
                        name=self._safe_str(raw_meta.get("name")),
                        alias=self._safe_str(raw_meta.get("alias")),
                        value_type=self._value_type(value),
                        value_text=self._value_text(value),
                        sort_order=index,
                    )
                )

        wholesale_payload = payload.get("wholesale_price")
        if isinstance(wholesale_payload, list):
            for index, raw_price in enumerate(wholesale_payload):
                if not isinstance(raw_price, dict):
                    continue
                wholesale_prices.append(
                    RunArtifactProductWholesalePrice(
                        from_items=self._as_float(raw_price.get("from_items")),
                        price=self._as_float(raw_price.get("price")),
                        sort_order=index,
                    )
                )

        for index, category_uid in enumerate(categories_uid):
            category_links.append(
                RunArtifactProductCategory(
                    category_uid=category_uid,
                    sort_order=index,
                )
            )

        return _PreparedProduct(
            product=product,
            images=images,
            metadata_entries=metadata_entries,
            wholesale_prices=wholesale_prices,
            category_links=category_links,
        )

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
    def _iter_chunks(items: list[Any], chunk_size: int):
        if chunk_size <= 0:
            if items:
                yield items
            return
        for index in range(0, len(items), chunk_size):
            yield items[index : index + chunk_size]

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
