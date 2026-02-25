from __future__ import annotations

import base64
import mimetypes
import tarfile
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin

import httpx


class ImagePipeline:
    def __init__(
        self,
        *,
        storage_base_url: str,
        storage_api_token: str,
        timeout_seconds: float = 30.0,
    ):
        self.storage_base_url = storage_base_url.rstrip("/")
        self.storage_api_token = storage_api_token
        self.timeout_seconds = timeout_seconds

    def process_images(self, images: Iterable[object]) -> list[dict[str, str | None]]:
        results: list[dict[str, str | None]] = []

        for image in images:
            filename = getattr(image, "filename", None) or "image.bin"
            content_base64 = getattr(image, "content_base64", None)
            result: dict[str, str | None] = {
                "filename": filename,
                "uploaded_url": None,
                "error": None,
            }

            try:
                if not content_base64:
                    raise ValueError("content_base64 is required for direct image upload")
                raw_bytes = base64.b64decode(content_base64, validate=True)
                result["uploaded_url"] = self._upload_to_storage(
                    filename=filename,
                    image_bytes=raw_bytes,
                )
            except Exception as exc:
                result["error"] = str(exc)

            results.append(result)

        return results

    def process_archive_images(self, archive_path: str | None) -> list[dict[str, str | None]]:
        if not archive_path:
            return []

        path = Path(archive_path).expanduser().resolve()
        if not path.is_file():
            return [
                {
                    "filename": None,
                    "uploaded_url": None,
                    "error": f"Archive not found: {path}",
                }
            ]

        results: list[dict[str, str | None]] = []
        try:
            with tarfile.open(path, mode="r:gz") as archive:
                for member in archive.getmembers():
                    if not member.isfile():
                        continue
                    if not member.name.startswith("images/"):
                        continue

                    filename = Path(member.name).name
                    if not filename:
                        continue

                    result: dict[str, str | None] = {
                        "filename": filename,
                        "uploaded_url": None,
                        "error": None,
                    }
                    try:
                        extracted = archive.extractfile(member)
                        if extracted is None:
                            raise ValueError("Archive entry has no data stream")
                        raw_bytes = extracted.read()
                        result["uploaded_url"] = self._upload_to_storage(
                            filename=filename,
                            image_bytes=raw_bytes,
                        )
                    except Exception as exc:
                        result["error"] = str(exc)

                    results.append(result)
        except Exception as exc:
            return [
                {
                    "filename": None,
                    "uploaded_url": None,
                    "error": f"Failed to read archive: {exc}",
                }
            ]

        return results

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
