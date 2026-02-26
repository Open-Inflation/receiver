from __future__ import annotations

import io
import json
import tarfile

from sqlalchemy import func, select

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

        assert categories_count == 2
        assert products_count == 1
        assert meta_count == 1
        assert wholesale_count == 1
        assert images_count == 2  # main + gallery
        assert product_categories_count == 2
    finally:
        session.close()


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
    finally:
        session.close()
