from __future__ import annotations

import base64
import threading
import time
from types import SimpleNamespace

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
