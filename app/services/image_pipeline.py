from __future__ import annotations

import asyncio
import base64
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
import hashlib
import logging
import mimetypes
import tarfile
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin

import httpx

LOGGER = logging.getLogger(__name__)


class ImagePipeline:
    def __init__(
        self,
        *,
        storage_base_url: str,
        storage_api_token: str,
        timeout_seconds: float = 30.0,
        max_parallel_uploads: int = 8,
        image_archive_max_file_bytes: int = 12 * 1024 * 1024,
        image_archive_max_files: int = 2000,
    ):
        self.storage_base_url = storage_base_url.rstrip("/")
        self.storage_api_token = storage_api_token
        self.timeout_seconds = timeout_seconds
        self.max_parallel_uploads = max(1, int(max_parallel_uploads))
        self.image_archive_max_file_bytes = max(1, int(image_archive_max_file_bytes))
        self.image_archive_max_files = max(1, int(image_archive_max_files))
        LOGGER.info(
            "ImagePipeline initialized: storage_base_url=%s max_parallel_uploads=%s timeout_seconds=%.1f image_archive_max_file_bytes=%s image_archive_max_files=%s",
            self.storage_base_url,
            self.max_parallel_uploads,
            self.timeout_seconds,
            self.image_archive_max_file_bytes,
            self.image_archive_max_files,
        )

    def process_images(self, images: Iterable[object]) -> list[dict[str, str | None]]:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.process_images_async(images))
        raise RuntimeError("Use async image pipeline methods in async context")

    async def process_images_async(self, images: Iterable[object]) -> list[dict[str, str | None]]:
        semaphore = asyncio.Semaphore(self.max_parallel_uploads)

        async def _process_one(image: object) -> dict[str, str | None]:
            filename = getattr(image, "filename", None) or "image.bin"
            content_base64 = getattr(image, "content_base64", None)
            result: dict[str, str | None] = {
                "filename": filename,
                "source_path": filename,
                "uploaded_url": None,
                "error": None,
            }

            try:
                if not content_base64:
                    raise ValueError("content_base64 is required for direct image upload")
                raw_bytes = base64.b64decode(content_base64, validate=True)
                async with semaphore:
                    result["uploaded_url"] = await asyncio.to_thread(
                        self._upload_to_storage,
                        filename=filename,
                        image_bytes=raw_bytes,
                    )
            except Exception as exc:
                result["error"] = str(exc)
                LOGGER.warning("Direct image upload failed: filename=%s error=%s", filename, result["error"])

            return result

        tasks = [_process_one(image) for image in images]
        if not tasks:
            return []
        results = await asyncio.gather(*tasks)
        success_count = sum(1 for item in results if item.get("uploaded_url"))
        LOGGER.info(
            "Direct image upload batch finished: total=%s success=%s failed=%s",
            len(results),
            success_count,
            len(results) - success_count,
        )
        return results

    def process_archive_images(self, archive_path: str | None) -> list[dict[str, str | None]]:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(self.process_archive_images_async(archive_path))
        raise RuntimeError("Use async image pipeline methods in async context")

    async def process_archive_images_async(self, archive_path: str | None) -> list[dict[str, str | None]]:
        if not archive_path:
            return []

        path = Path(archive_path).expanduser().resolve()
        if not path.is_file():
            LOGGER.warning("Archive image upload skipped: archive_not_found path=%s", path)
            return [
                {
                    "filename": None,
                    "uploaded_url": None,
                    "error": f"Archive not found: {path}",
                }
            ]

        try:
            results = await asyncio.to_thread(self._process_archive_images_threaded, path)
        except Exception as exc:
            LOGGER.warning("Archive image upload failed while reading archive: path=%s error=%s", path, exc)
            return [
                {
                    "filename": None,
                    "uploaded_url": None,
                    "error": f"Failed to read archive: {exc}",
                }
            ]
        if not results:
            return []
        for item in results:
            if item.get("error"):
                LOGGER.warning(
                    "Archive image upload failed: source_path=%s filename=%s error=%s",
                    item.get("source_path"),
                    item.get("filename"),
                    item.get("error"),
                )
        success_count = sum(1 for item in results if item.get("uploaded_url"))
        LOGGER.info(
            "Archive image upload batch finished: archive=%s total=%s success=%s failed=%s",
            path,
            len(results),
            success_count,
            len(results) - success_count,
        )
        return results

    def _process_archive_images_threaded(self, path: Path) -> list[dict[str, str | None]]:
        indexed_results: dict[int, dict[str, str | None]] = {}
        in_flight: dict[Future[dict[str, str | None]], int] = {}
        next_index = 0
        max_in_flight = self.max_parallel_uploads
        processed_files = 0

        with ThreadPoolExecutor(max_workers=self.max_parallel_uploads) as pool:
            with tarfile.open(path, mode="r|gz") as archive:
                for member in archive:
                    if not member.isfile():
                        continue
                    if not member.name.startswith("images/"):
                        continue
                    if processed_files >= self.image_archive_max_files:
                        index = next_index
                        next_index += 1
                        indexed_results[index] = {
                            "filename": None,
                            "source_path": member.name,
                            "uploaded_url": None,
                            "error": (
                                "Archive image file limit exceeded "
                                f"({self.image_archive_max_files})"
                            ),
                        }
                        break

                    filename = Path(member.name).name
                    if not filename:
                        continue

                    index = next_index
                    next_index += 1
                    processed_files += 1

                    if member.size > self.image_archive_max_file_bytes:
                        indexed_results[index] = {
                            "filename": filename,
                            "source_path": member.name,
                            "uploaded_url": None,
                            "error": (
                                f"Archive image is too large ({member.size} bytes), "
                                f"limit is {self.image_archive_max_file_bytes}"
                            ),
                        }
                        continue

                    try:
                        extracted = archive.extractfile(member)
                        if extracted is None:
                            indexed_results[index] = {
                                "filename": filename,
                                "source_path": member.name,
                                "uploaded_url": None,
                                "error": "Archive entry has no data stream",
                            }
                            continue
                        raw_bytes = extracted.read(self.image_archive_max_file_bytes + 1)
                        if len(raw_bytes) > self.image_archive_max_file_bytes:
                            indexed_results[index] = {
                                "filename": filename,
                                "source_path": member.name,
                                "uploaded_url": None,
                                "error": (
                                    "Archive image payload exceeds size limit "
                                    f"{self.image_archive_max_file_bytes}"
                                ),
                            }
                            continue
                    except Exception as exc:
                        indexed_results[index] = {
                            "filename": filename,
                            "source_path": member.name,
                            "uploaded_url": None,
                            "error": str(exc),
                        }
                        continue

                    future = pool.submit(
                        self._upload_archive_item,
                        filename=filename,
                        source_path=member.name,
                        raw_bytes=raw_bytes,
                    )
                    in_flight[future] = index

                    if len(in_flight) >= max_in_flight:
                        done, _ = wait(set(in_flight), return_when=FIRST_COMPLETED)
                        self._collect_completed_archive_uploads(
                            completed=done,
                            in_flight=in_flight,
                            indexed_results=indexed_results,
                        )

            if in_flight:
                done, _ = wait(set(in_flight))
                self._collect_completed_archive_uploads(
                    completed=done,
                    in_flight=in_flight,
                    indexed_results=indexed_results,
                )

        return [indexed_results[index] for index in range(next_index)]

    def _collect_completed_archive_uploads(
        self,
        *,
        completed: set[Future[dict[str, str | None]]],
        in_flight: dict[Future[dict[str, str | None]], int],
        indexed_results: dict[int, dict[str, str | None]],
    ) -> None:
        for future in completed:
            index = in_flight.pop(future)
            try:
                indexed_results[index] = future.result()
            except Exception as exc:
                indexed_results[index] = {
                    "filename": None,
                    "source_path": None,
                    "uploaded_url": None,
                    "error": str(exc),
                }

    def _upload_archive_item(
        self,
        *,
        filename: str,
        source_path: str,
        raw_bytes: bytes,
    ) -> dict[str, str | None]:
        result: dict[str, str | None] = {
            "filename": filename,
            "source_path": source_path,
            "uploaded_url": None,
            "error": None,
        }
        try:
            result["uploaded_url"] = self._upload_to_storage(
                filename=filename,
                image_bytes=raw_bytes,
            )
        except Exception as exc:
            result["error"] = str(exc)
        return result

    def _upload_to_storage(self, *, filename: str, image_bytes: bytes) -> str:
        headers = {"Authorization": f"Bearer {self.storage_api_token}"}
        mime_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        files = {"file": (filename, image_bytes, mime_type)}
        image_name = self._build_storage_image_name(filename=filename, image_bytes=image_bytes)

        with httpx.Client(
            base_url=self.storage_base_url,
            follow_redirects=False,
            timeout=self.timeout_seconds,
        ) as client:
            response = client.post(f"/api/images/{image_name}", headers=headers, files=files)

        if response.status_code == 409:
            return urljoin(f"{self.storage_base_url}/", f"images/{image_name}")

        if response.status_code not in {200, 201, 302, 303}:
            raise RuntimeError(self._format_upload_error(response))

        return self._extract_uploaded_url(response, fallback_path=f"/images/{image_name}")

    def _build_storage_image_name(self, *, filename: str, image_bytes: bytes) -> str:
        stem = Path(filename).stem.strip().lower() or "image"
        safe_stem = "".join(ch if ("a" <= ch <= "z" or "0" <= ch <= "9") else "_" for ch in stem)
        while "__" in safe_stem:
            safe_stem = safe_stem.replace("__", "_")
        safe_stem = safe_stem.strip("_") or "image"
        safe_stem = safe_stem[:48]
        payload_digest = hashlib.sha256(image_bytes).hexdigest()[:24]
        return f"{safe_stem}_{payload_digest}.webp"

    def _extract_uploaded_url(self, response: httpx.Response, *, fallback_path: str | None = None) -> str:
        location = response.headers.get("location")
        if location:
            return urljoin(f"{self.storage_base_url}/", location.lstrip("/"))

        content_type = response.headers.get("content-type", "")
        if "application/json" in content_type.lower():
            try:
                body = response.json()
                url_value = body.get("url")
                if isinstance(url_value, str) and url_value.strip():
                    return url_value
            except ValueError:
                pass

        if fallback_path is not None:
            return urljoin(f"{self.storage_base_url}/", fallback_path.lstrip("/"))

        raise RuntimeError("Storage response does not include image URL")

    @staticmethod
    def _format_upload_error(response: httpx.Response) -> str:
        detail = ""
        content_type = response.headers.get("content-type", "")
        if "application/json" in content_type.lower():
            try:
                body = response.json()
                if isinstance(body, dict):
                    detail = str(body.get("detail") or body.get("message") or "").strip()
            except ValueError:
                detail = ""
        if not detail:
            detail = response.text.strip()[:300]
        if detail:
            return f"Storage upload failed with status {response.status_code}: {detail}"
        return f"Storage upload failed with status {response.status_code}"
