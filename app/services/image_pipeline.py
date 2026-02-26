from __future__ import annotations

import asyncio
import base64
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
    ):
        self.storage_base_url = storage_base_url.rstrip("/")
        self.storage_api_token = storage_api_token
        self.timeout_seconds = timeout_seconds
        self.max_parallel_uploads = max(1, int(max_parallel_uploads))
        LOGGER.info(
            "ImagePipeline initialized: storage_base_url=%s max_parallel_uploads=%s timeout_seconds=%.1f",
            self.storage_base_url,
            self.max_parallel_uploads,
            self.timeout_seconds,
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
            archive_items = await asyncio.to_thread(self._read_archive_items, path)
        except Exception as exc:
            LOGGER.warning("Archive image upload failed while reading archive: path=%s error=%s", path, exc)
            return [
                {
                    "filename": None,
                    "uploaded_url": None,
                    "error": f"Failed to read archive: {exc}",
                }
            ]

        semaphore = asyncio.Semaphore(self.max_parallel_uploads)

        async def _process_one(item: tuple[str, str, bytes | None, str | None]) -> dict[str, str | None]:
            filename, source_path, raw_bytes, read_error = item
            result: dict[str, str | None] = {
                "filename": filename,
                "source_path": source_path,
                "uploaded_url": None,
                "error": read_error,
            }
            if read_error is not None:
                return result
            if raw_bytes is None:
                result["error"] = "Archive entry has no data stream"
                return result

            try:
                async with semaphore:
                    result["uploaded_url"] = await asyncio.to_thread(
                        self._upload_to_storage,
                        filename=filename,
                        image_bytes=raw_bytes,
                    )
            except Exception as exc:
                result["error"] = str(exc)
                LOGGER.warning(
                    "Archive image upload failed: source_path=%s filename=%s error=%s",
                    source_path,
                    filename,
                    result["error"],
                )
            return result

        tasks = [_process_one(item) for item in archive_items]
        if not tasks:
            return []
        results = await asyncio.gather(*tasks)
        success_count = sum(1 for item in results if item.get("uploaded_url"))
        LOGGER.info(
            "Archive image upload batch finished: archive=%s total=%s success=%s failed=%s",
            path,
            len(results),
            success_count,
            len(results) - success_count,
        )
        return results

    def _read_archive_items(self, path: Path) -> list[tuple[str, str, bytes | None, str | None]]:
        items: list[tuple[str, str, bytes | None, str | None]] = []
        with tarfile.open(path, mode="r:gz") as archive:
            for member in archive.getmembers():
                if not member.isfile():
                    continue
                if not member.name.startswith("images/"):
                    continue

                filename = Path(member.name).name
                if not filename:
                    continue

                try:
                    extracted = archive.extractfile(member)
                    if extracted is None:
                        items.append((filename, member.name, None, "Archive entry has no data stream"))
                        continue
                    items.append((filename, member.name, extracted.read(), None))
                except Exception as exc:
                    items.append((filename, member.name, None, str(exc)))
        return items

    def _upload_to_storage(self, *, filename: str, image_bytes: bytes) -> str:
        headers = {"Authorization": f"Bearer {self.storage_api_token}"}
        mime_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        files = {"file": (filename, image_bytes, mime_type)}

        with httpx.Client(
            base_url=self.storage_base_url,
            follow_redirects=False,
            timeout=self.timeout_seconds,
        ) as client:
            response = client.post("/api/images", headers=headers, files=files)

        if response.status_code not in {200, 201, 302, 303}:
            raise RuntimeError(f"Storage upload failed with status {response.status_code}")

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

        raise RuntimeError("Storage response does not include image URL")
