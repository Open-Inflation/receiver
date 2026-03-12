from __future__ import annotations

import io
import json
from pathlib import Path
import tarfile

from sqlalchemy import func, select

from app.services.artifact_ingestor import ArtifactIngestor
from app.models import (
    RunArtifact,
    RunArtifactCategory,
    RunArtifactProduct,
    RunArtifactProductCategory,
    RunArtifactProductImage,
    RunArtifactProductMeta,
    RunArtifactProductWholesalePrice,
    TaskRun,
)


def _create_assigned_run(client, *, city: str, store: str, orchestrator_name: str) -> str:
    create_task = client.post(
        "/api/tasks",
        json={
            "city": city,
            "store": store,
            "frequency_hours": 24,
        },
    )
    assert create_task.status_code == 201

    token = _register_orchestrator(client, name=orchestrator_name)
    assignment = client.post("/api/orchestrators/next-task", headers=_auth(token)).json()["assignment"]
    assert assignment is not None
    return str(assignment["run_id"])


def _register_orchestrator(client, name: str = "orch-artifact") -> str:
    response = client.post("/api/orchestrators/register", json={"name": name})
    assert response.status_code == 200
    return response.json()["token"]


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def test_success_result_ingests_json_to_normalized_tables(client, tmp_path):
    artifact_payload = {
        "retail_type": "store",
        "code": "C001",
        "address": "Moscow, Test st. 1",
        "schedule_weekdays": {"open_from": "09:00", "closed_from": "22:00"},
        "schedule_saturday": {"open_from": "10:00", "closed_from": "21:00"},
        "schedule_sunday": {"open_from": "11:00", "closed_from": "20:00"},
        "temporarily_closed": False,
        "longitude": 37.62,
        "latitude": 55.75,
        "administrative_unit": {
            "settlement_type": "city",
            "name": "Москва",
            "alias": None,
            "country": "RUS",
            "region": "г. Москва",
            "longitude": 37.6176,
            "latitude": 55.7558,
        },
        "categories": [
            {
                "uid": "cat-root",
                "alias": "root",
                "title": "Root",
                "adult": False,
                "icon": None,
                "banner": None,
                "children": [
                    {
                        "uid": "cat-child",
                        "alias": "child",
                        "title": "Child",
                        "adult": False,
                        "icon": None,
                        "banner": None,
                        "children": [],
                    }
                ],
            }
        ],
        "products": [
            {
                "sku": "SKU-1",
                "plu": "1001",
                "source_page_url": "https://example.test/p/sku-1",
                "title": "Product 1",
                "description": "Description 1",
                "adult": False,
                "new": True,
                "promo": False,
                "season": False,
                "hit": True,
                "data_matrix": True,
                "brand": "Brand",
                "producer_name": "Producer",
                "producer_country": "RUS",
                "composition": "Milk",
                "meta_data": [{"name": "Weight", "alias": "weight", "value": "1kg"}],
                "expiration_date_in_days": 10,
                "rating": 4.7,
                "reviews_count": 12,
                "price": 99.9,
                "discount_price": 89.9,
                "loyal_price": 79.9,
                "wholesale_price": [{"from_items": 3, "price": 74.5}],
                "price_unit": "RUB",
                "unit": "PCE",
                "available_count": 5,
                "package_quantity": 1.0,
                "package_unit": "LTR",
                "categories_uid": ["cat-root", "cat-child"],
                "main_image": "images/main.jpg",
                "images": ["images/gallery_1.jpg"],
            }
        ],
    }

    artifact_path = tmp_path / "artifact.json"
    artifact_path.write_text(json.dumps(artifact_payload, ensure_ascii=False), encoding="utf-8")
    archive_path = tmp_path / "artifact.tar.gz"
    gallery_bytes = b"gallery-bytes"
    main_bytes = b"main-bytes"
    with tarfile.open(archive_path, mode="w:gz") as archive:
        member_main = tarfile.TarInfo(name="images/main.jpg")
        member_main.size = len(main_bytes)
        archive.addfile(member_main, io.BytesIO(main_bytes))

        member_gallery = tarfile.TarInfo(name="images/gallery_1.jpg")
        member_gallery.size = len(gallery_bytes)
        archive.addfile(member_gallery, io.BytesIO(gallery_bytes))

    def fake_upload(*, filename: str, image_bytes: bytes) -> str:
        if filename == "main.jpg":
            assert image_bytes == main_bytes
            return "http://storage.local/images/main.jpg"
        if filename == "gallery_1.jpg":
            assert image_bytes == gallery_bytes
            return "http://storage.local/images/gallery_1.jpg"
        raise AssertionError(f"unexpected filename {filename}")

    client.app.state.image_pipeline._upload_to_storage = fake_upload

    create_task = client.post(
        "/api/tasks",
        json={
            "city": "Moscow",
            "store": "C001",
            "frequency_hours": 24,
        },
    )
    assert create_task.status_code == 201

    token = _register_orchestrator(client)
    assignment = client.post("/api/orchestrators/next-task", headers=_auth(token)).json()["assignment"]
    assert assignment is not None
    run_id = assignment["run_id"]

    finish = client.post(
        "/api/orchestrators/results",
        headers=_auth(token),
        json={
            "run_id": run_id,
            "status": "success",
            "output_json": str(artifact_path),
            "output_gz": str(archive_path),
            "upload_images_from_archive": True,
        },
    )
    assert finish.status_code == 200

    session = client.app.state.session_factory()
    try:
        artifact = session.scalar(select(RunArtifact).where(RunArtifact.run_id == run_id))
        assert artifact is not None
        assert artifact.source == "output_json"
        assert artifact.parser_name == "fixprice"
        assert artifact.code == "C001"

        product = session.scalar(
            select(RunArtifactProduct).where(RunArtifactProduct.artifact_id == artifact.id)
        )
        assert product is not None
        assert product.main_image == "http://storage.local/images/main.jpg"

        product_images = session.scalars(
            select(RunArtifactProductImage)
            .where(RunArtifactProductImage.product_id == product.id)
            .order_by(RunArtifactProductImage.sort_order.asc())
        ).all()
        assert [row.url for row in product_images] == [
            "http://storage.local/images/main.jpg",
            "http://storage.local/images/gallery_1.jpg",
        ]

        categories_count = session.scalar(
            select(func.count(RunArtifactCategory.id)).where(RunArtifactCategory.artifact_id == artifact.id)
        )
        products_count = session.scalar(
            select(func.count(RunArtifactProduct.id)).where(RunArtifactProduct.artifact_id == artifact.id)
        )
        meta_count = session.scalar(select(func.count(RunArtifactProductMeta.id)))
        wholesale_count = session.scalar(select(func.count(RunArtifactProductWholesalePrice.id)))
        images_count = session.scalar(select(func.count(RunArtifactProductImage.id)))
        product_categories_count = session.scalar(select(func.count(RunArtifactProductCategory.id)))
        run = session.get(TaskRun, run_id)

        assert categories_count == 2
        assert products_count == 1
        assert meta_count == 1
        assert wholesale_count == 1
        assert images_count == 2  # main + gallery
        assert product_categories_count == 2
        assert run is not None
        assert run.artifact_source == "output_json"
        assert int(run.artifact_products_count) == 1
        assert int(run.artifact_categories_count) == 2
        assert run.artifact_dataclass_validated in {True, False}
        assert run.artifact_ingested_at is not None
    finally:
        session.close()


def test_artifact_download_size_limit(monkeypatch):
    ingestor = ArtifactIngestor(
        parser_src_path=(Path(__file__).resolve().parents[2] / "parser" / "src"),
        download_max_bytes=5,
        json_member_max_bytes=1024,
    )

    class _FakeResponse:
        def __init__(self):
            self._chunks = [b"123", b"456"]

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def raise_for_status(self):
            return None

        def iter_bytes(self):
            for chunk in self._chunks:
                yield chunk

    class _FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def stream(self, _method, _url):
            return _FakeResponse()

    monkeypatch.setattr("app.services.artifact_ingestor.httpx.Client", _FakeClient)

    payload, error = ingestor._load_from_download_url("http://example.test/artifact", expected_sha256=None)
    assert payload is None
    assert error is not None
    assert "exceeds limit" in error.lower()


def test_artifact_json_member_size_limit():
    ingestor = ArtifactIngestor(
        parser_src_path=(Path(__file__).resolve().parents[2] / "parser" / "src"),
        download_max_bytes=1024 * 1024,
        json_member_max_bytes=24,
    )

    oversized_payload = {"key": "x" * 200}
    archive_bytes = io.BytesIO()
    encoded = json.dumps(oversized_payload).encode("utf-8")
    with tarfile.open(fileobj=archive_bytes, mode="w:gz") as archive:
        member = tarfile.TarInfo(name="meta.json")
        member.size = len(encoded)
        archive.addfile(member, io.BytesIO(encoded))

    payload, error = ingestor._extract_json_payload(archive_bytes.getvalue())
    assert payload is None
    assert error is not None
    assert "too large" in error.lower()


def test_ingest_is_idempotent_for_same_run(client, tmp_path):
    artifact_payload = {
        "retail_type": "store",
        "code": "C002",
        "address": "Moscow",
        "schedule_weekdays": {"open_from": "09:00", "closed_from": "20:00"},
        "schedule_saturday": {"open_from": "09:00", "closed_from": "20:00"},
        "schedule_sunday": {"open_from": "09:00", "closed_from": "20:00"},
        "temporarily_closed": False,
        "longitude": 37.6,
        "latitude": 55.7,
        "administrative_unit": {
            "settlement_type": "city",
            "name": "Москва",
            "alias": None,
            "country": "RUS",
            "region": "г. Москва",
            "longitude": 37.6,
            "latitude": 55.7,
        },
        "categories": [],
        "products": [],
    }

    artifact_path = tmp_path / "artifact-idempotent.json"
    artifact_path.write_text(json.dumps(artifact_payload, ensure_ascii=False), encoding="utf-8")

    task_response = client.post(
        "/api/tasks",
        json={
            "city": "Moscow",
            "store": "C002",
            "frequency_hours": 24,
        },
    )
    assert task_response.status_code == 201

    token = _register_orchestrator(client, name="orch-idempotent")
    assignment = client.post("/api/orchestrators/next-task", headers=_auth(token)).json()["assignment"]
    assert assignment is not None
    run_id = assignment["run_id"]

    finish = client.post(
        "/api/orchestrators/results",
        headers=_auth(token),
        json={
            "run_id": run_id,
            "status": "success",
            "output_json": str(artifact_path),
        },
    )
    assert finish.status_code == 200

    session = client.app.state.session_factory()
    try:
        run = session.get(TaskRun, run_id)
        assert run is not None
        result = client.app.state.artifact_ingestor.ingest_run_output(
            session,
            run=run,
            output_json=str(artifact_path),
        )
        assert result["ok"] is True

        artifact_rows = session.scalar(
            select(func.count(RunArtifact.id)).where(RunArtifact.run_id == run_id)
        )
        assert artifact_rows == 1
        artifact = session.scalar(select(RunArtifact).where(RunArtifact.run_id == run_id))
        assert artifact is not None
        assert artifact.parser_name == "fixprice"
        refreshed_run = session.get(TaskRun, run_id)
        assert refreshed_run is not None
        assert refreshed_run.artifact_source == "output_json"
    finally:
        session.close()


def test_large_payload_ingests_with_small_chunks(client, tmp_path):
    categories = []
    for idx in range(6):
        categories.append(
            {
                "uid": f"cat-{idx}",
                "alias": f"alias-{idx}",
                "title": f"Category {idx}",
                "adult": False,
                "icon": None,
                "banner": None,
                "children": [],
            }
        )

    products = []
    for idx in range(5):
        products.append(
            {
                "sku": f"SKU-{idx}",
                "plu": f"PLU-{idx}",
                "source_page_url": f"https://example.test/p/{idx}",
                "title": f"Product {idx}",
                "description": f"Description {idx}",
                "adult": False,
                "new": idx % 2 == 0,
                "promo": False,
                "season": False,
                "hit": True,
                "data_matrix": True,
                "brand": "Brand",
                "producer_name": "Producer",
                "producer_country": "RUS",
                "composition": "Milk",
                "meta_data": [
                    {"name": "Weight", "alias": "weight", "value": "1kg"},
                    {"name": "Color", "alias": "color", "value": "white"},
                ],
                "expiration_date_in_days": 10,
                "rating": 4.5,
                "reviews_count": 8,
                "price": 100 + idx,
                "discount_price": 90 + idx,
                "loyal_price": 80 + idx,
                "wholesale_price": [{"from_items": 3, "price": 70 + idx}],
                "price_unit": "RUB",
                "unit": "PCE",
                "available_count": 5,
                "package_quantity": 1.0,
                "package_unit": "LTR",
                "categories_uid": [f"cat-{idx}", f"cat-{(idx + 1) % 6}"],
                "main_image": f"images/main_{idx}.jpg",
                "images": [f"images/extra_{idx}.jpg"],
            }
        )

    artifact_payload = {
        "retail_type": "store",
        "code": "C-LARGE",
        "address": "Moscow, Chunk st. 1",
        "schedule_weekdays": {"open_from": "09:00", "closed_from": "22:00"},
        "schedule_saturday": {"open_from": "10:00", "closed_from": "21:00"},
        "schedule_sunday": {"open_from": "11:00", "closed_from": "20:00"},
        "temporarily_closed": False,
        "longitude": 37.62,
        "latitude": 55.75,
        "administrative_unit": {
            "settlement_type": "city",
            "name": "Москва",
            "alias": None,
            "country": "RUS",
            "region": "г. Москва",
            "longitude": 37.6176,
            "latitude": 55.7558,
        },
        "categories": categories,
        "products": products,
    }
    artifact_path = tmp_path / "artifact-large.json"
    artifact_path.write_text(json.dumps(artifact_payload, ensure_ascii=False), encoding="utf-8")

    run_id = _create_assigned_run(
        client,
        city="Moscow",
        store="C-LARGE",
        orchestrator_name="orch-chunk-ingest",
    )

    ingestor = client.app.state.artifact_ingestor
    ingestor._products_per_txn = 2
    ingestor._categories_per_txn = 2
    ingestor._relations_per_txn = 3

    session = client.app.state.session_factory()
    try:
        run = session.get(TaskRun, run_id)
        assert run is not None
        result = ingestor.ingest_run_output(session, run=run, output_json=str(artifact_path))
        assert result["ok"] is True

        artifact = session.scalar(select(RunArtifact).where(RunArtifact.run_id == run_id))
        assert artifact is not None
        categories_count = session.scalar(
            select(func.count(RunArtifactCategory.id)).where(RunArtifactCategory.artifact_id == artifact.id)
        )
        products_count = session.scalar(
            select(func.count(RunArtifactProduct.id)).where(RunArtifactProduct.artifact_id == artifact.id)
        )
        meta_count = session.scalar(select(func.count(RunArtifactProductMeta.id)))
        wholesale_count = session.scalar(select(func.count(RunArtifactProductWholesalePrice.id)))
        images_count = session.scalar(select(func.count(RunArtifactProductImage.id)))
        product_categories_count = session.scalar(select(func.count(RunArtifactProductCategory.id)))
        refreshed_run = session.get(TaskRun, run_id)

        assert categories_count == 6
        assert products_count == 5
        assert meta_count == 10
        assert wholesale_count == 5
        assert images_count == 10
        assert product_categories_count == 10
        assert refreshed_run is not None
        assert int(refreshed_run.artifact_products_count) == 5
        assert int(refreshed_run.artifact_categories_count) == 6
    finally:
        session.close()


def test_ingest_cleanup_removes_partial_artifact_on_chunk_failure(client, tmp_path):
    artifact_payload = {
        "retail_type": "store",
        "code": "C-FAIL",
        "address": "Moscow, Fail st. 1",
        "schedule_weekdays": {"open_from": "09:00", "closed_from": "22:00"},
        "schedule_saturday": {"open_from": "10:00", "closed_from": "21:00"},
        "schedule_sunday": {"open_from": "11:00", "closed_from": "20:00"},
        "temporarily_closed": False,
        "longitude": 37.62,
        "latitude": 55.75,
        "categories": [{"uid": "cat-fail", "title": "Fail", "children": []}],
        "products": [
            {
                "sku": "FAIL-1",
                "title": "Fail product 1",
                "unit": "PCE",
                "images": ["images/a.jpg"],
                "categories_uid": ["cat-fail"],
            },
            {
                "sku": "FAIL-2",
                "title": "Fail product 2",
                "unit": "PCE",
                "images": ["images/b.jpg"],
                "categories_uid": ["cat-fail"],
            },
        ],
    }
    artifact_path = tmp_path / "artifact-fail.json"
    artifact_path.write_text(json.dumps(artifact_payload, ensure_ascii=False), encoding="utf-8")

    run_id = _create_assigned_run(
        client,
        city="Moscow",
        store="C-FAIL",
        orchestrator_name="orch-chunk-fail",
    )

    ingestor = client.app.state.artifact_ingestor
    ingestor._products_per_txn = 1
    ingestor._relations_per_txn = 1
    original_persist_relations = ingestor._persist_product_relations

    def _failing_persist_relations(*args, **kwargs):
        raise RuntimeError("forced relation failure")

    ingestor._persist_product_relations = _failing_persist_relations

    session = client.app.state.session_factory()
    try:
        run = session.get(TaskRun, run_id)
        assert run is not None
        try:
            ingestor.ingest_run_output(session, run=run, output_json=str(artifact_path))
            raise AssertionError("Expected ingest failure")
        except RuntimeError as exc:
            assert "forced relation failure" in str(exc)
    finally:
        ingestor._persist_product_relations = original_persist_relations
        session.close()

    verify_session = client.app.state.session_factory()
    try:
        artifact_count = verify_session.scalar(select(func.count(RunArtifact.id)).where(RunArtifact.run_id == run_id))
        assert artifact_count == 0
    finally:
        verify_session.close()
