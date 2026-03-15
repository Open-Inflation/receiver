from __future__ import annotations

import base64
import io
import threading
import tarfile
import time
from pathlib import Path
from types import SimpleNamespace

import app.services.image_pipeline as image_pipeline_module
from app.services.image_pipeline import ImagePipeline


def test_process_images_uploads_in_parallel(monkeypatch):
    pipeline = ImagePipeline(
        storage_base_url="http://storage.local",
        storage_api_token="token",
        max_parallel_uploads=6,
    )

    active_uploads = 0
    max_active_uploads = 0
    lock = threading.Lock()

    def fake_upload(*, filename: str, image_bytes: bytes) -> str:
        nonlocal active_uploads, max_active_uploads
        assert image_bytes
        with lock:
            active_uploads += 1
            max_active_uploads = max(max_active_uploads, active_uploads)
        try:
            time.sleep(0.05)
            return f"http://storage.local/images/{filename}"
        finally:
            with lock:
                active_uploads -= 1

    monkeypatch.setattr(pipeline, "_upload_to_storage", fake_upload)

    payload = base64.b64encode(b"img-bytes").decode("ascii")
    images = [
        SimpleNamespace(filename=f"img-{index}.jpg", content_base64=payload)
        for index in range(8)
    ]

    results = pipeline.process_images(images)
    assert len(results) == 8
    assert all(item.get("uploaded_url") for item in results)
    assert all(item.get("error") is None for item in results)
    assert max_active_uploads >= 2


def _write_archive(path: Path, files: list[tuple[str, bytes]]) -> None:
    with tarfile.open(path, mode="w:gz") as archive:
        for name, payload in files:
            member = tarfile.TarInfo(name=f"images/{name}")
            member.size = len(payload)
            archive.addfile(member, io.BytesIO(payload))


def test_process_archive_images_skips_oversized_file(tmp_path, monkeypatch):
    archive_path = tmp_path / "oversized.tar.gz"
    _write_archive(archive_path, [("big.jpg", b"123456")])

    pipeline = ImagePipeline(
        storage_base_url="http://storage.local",
        storage_api_token="token",
        max_parallel_uploads=2,
        image_archive_max_file_bytes=4,
        image_archive_max_files=10,
    )

    def fail_upload(*, filename: str, image_bytes: bytes) -> str:
        raise AssertionError(f"upload must not be called for {filename}")

    monkeypatch.setattr(pipeline, "_upload_to_storage", fail_upload)
    results = pipeline.process_archive_images(str(archive_path))

    assert len(results) == 1
    assert results[0]["uploaded_url"] is None
    assert "too large" in str(results[0]["error"]).lower()


def test_process_archive_images_stops_after_max_files(tmp_path, monkeypatch):
    archive_path = tmp_path / "limit.tar.gz"
    _write_archive(
        archive_path,
        [
            ("a.jpg", b"a"),
            ("b.jpg", b"b"),
            ("c.jpg", b"c"),
        ],
    )

    pipeline = ImagePipeline(
        storage_base_url="http://storage.local",
        storage_api_token="token",
        max_parallel_uploads=2,
        image_archive_max_file_bytes=32,
        image_archive_max_files=2,
    )
    monkeypatch.setattr(
        pipeline,
        "_upload_to_storage",
        lambda *, filename, image_bytes: f"http://storage.local/images/{filename}",
    )

    results = pipeline.process_archive_images(str(archive_path))
    uploaded = [item for item in results if item.get("uploaded_url")]
    errors = [item for item in results if item.get("error")]

    assert len(uploaded) == 2
    assert len(errors) == 1
    assert "limit exceeded" in str(errors[0]["error"]).lower()


def test_upload_to_storage_uses_named_endpoint(monkeypatch):
    pipeline = ImagePipeline(storage_base_url="http://storage.local", storage_api_token="token")
    requests: list[str] = []

    class _FakeResponse:
        def __init__(self, status_code: int, *, headers: dict[str, str] | None = None):
            self.status_code = status_code
            self.headers = headers or {}
            self.text = ""

        def json(self):
            return {}

    class _FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def post(self, path: str, *, headers, files):
            requests.append(path)
            assert path.startswith("/api/images/")
            assert path.endswith(".webp")
            return _FakeResponse(201, headers={"location": "/images/uploaded.webp"})

    monkeypatch.setattr(image_pipeline_module.httpx, "Client", lambda **kwargs: _FakeClient())

    uploaded = pipeline._upload_to_storage(filename="main.jpg", image_bytes=b"img")

    assert uploaded == "http://storage.local/images/uploaded.webp"
    assert len(requests) == 1
    assert requests[0].startswith("/api/images/")


def test_upload_to_storage_named_conflict_returns_existing_url(monkeypatch):
    pipeline = ImagePipeline(storage_base_url="http://storage.local", storage_api_token="token")
    requests: list[str] = []

    class _FakeResponse:
        def __init__(self, status_code: int, *, headers: dict[str, str] | None = None):
            self.status_code = status_code
            self.headers = headers or {}
            self.text = ""

        def json(self):
            return {}

    class _FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def post(self, path: str, *, headers, files):
            requests.append(path)
            return _FakeResponse(409)

    monkeypatch.setattr(image_pipeline_module.httpx, "Client", lambda **kwargs: _FakeClient())

    image_bytes = b"same-image"
    expected_name = pipeline._build_storage_image_name(filename="main.jpg", image_bytes=image_bytes)
    uploaded = pipeline._upload_to_storage(filename="main.jpg", image_bytes=image_bytes)

    assert uploaded == f"http://storage.local/images/{expected_name}"
    assert requests == [f"/api/images/{expected_name}"]
