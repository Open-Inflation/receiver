from __future__ import annotations

import io
import tarfile
from datetime import datetime, timedelta, timezone


def _register_orchestrator(client, name: str = "orch-main") -> str:
    response = client.post("/api/orchestrators/register", json={"name": name})
    assert response.status_code == 200
    return response.json()["token"]


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def test_task_lifecycle_success(client):
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

    next_task = client.post("/api/orchestrators/next-task", headers=_auth(token))
    assert next_task.status_code == 200
    assignment = next_task.json()["assignment"]
    assert assignment is not None
    run_id = assignment["run_id"]

    second_claim = client.post("/api/orchestrators/next-task", headers=_auth(token))
    assert second_claim.status_code == 200
    assert second_claim.json()["assignment"] is None

    finish = client.post(
        "/api/orchestrators/results",
        headers=_auth(token),
        json={
            "run_id": run_id,
            "status": "success",
            "payload": {
                "city_id": "3",
                "action": "ping",
            },
        },
    )
    assert finish.status_code == 200
    body = finish.json()
    assert body["run_id"] == run_id
    assert body["status"] == "success"

    runs = client.get(f"/api/runs/{run_id}", headers=_auth(token))
    assert runs.status_code == 200
    assert runs.json()["status"] == "success"

    tasks = client.get("/api/tasks")
    assert tasks.status_code == 200
    stored_task = tasks.json()[0]
    assert stored_task["last_crawl_at"] is not None
    assert stored_task["lease_owner_id"] is None

    no_due_tasks = client.post("/api/orchestrators/next-task", headers=_auth(token))
    assert no_due_tasks.status_code == 200
    assert no_due_tasks.json()["assignment"] is None


def test_due_task_selected_when_frequency_elapsed(client):
    old_timestamp = (datetime.now(timezone.utc) - timedelta(hours=5)).isoformat()
    create_task = client.post(
        "/api/tasks",
        json={
            "city": "SPB",
            "store": "A100",
            "frequency_hours": 1,
            "last_crawl_at": old_timestamp,
        },
    )
    assert create_task.status_code == 201

    token = _register_orchestrator(client, name="orch-frequency")

    next_task = client.post("/api/orchestrators/next-task", headers=_auth(token))
    assert next_task.status_code == 200
    assert next_task.json()["assignment"] is not None


def test_delete_task_removes_it_from_list(client):
    create_task = client.post(
        "/api/tasks",
        json={
            "city": "Moscow",
            "store": "D100",
            "frequency_hours": 24,
        },
    )
    assert create_task.status_code == 201
    task_id = create_task.json()["id"]

    deleted = client.delete(f"/api/tasks/{task_id}")
    assert deleted.status_code == 200
    assert deleted.json()["ok"] is True

    tasks = client.get("/api/tasks")
    assert tasks.status_code == 200
    assert tasks.json() == []


def test_delete_task_keeps_existing_run_history(client):
    create_task = client.post(
        "/api/tasks",
        json={
            "city": "Moscow",
            "store": "H100",
            "frequency_hours": 24,
        },
    )
    assert create_task.status_code == 201
    task_id = create_task.json()["id"]

    token = _register_orchestrator(client, name="orch-delete-history")
    assignment = client.post("/api/orchestrators/next-task", headers=_auth(token)).json()["assignment"]
    assert assignment is not None
    run_id = assignment["run_id"]

    deleted = client.delete(f"/api/tasks/{task_id}")
    assert deleted.status_code == 200
    assert deleted.json()["ok"] is True

    tasks = client.get("/api/tasks")
    assert tasks.status_code == 200
    assert tasks.json() == []

    run = client.get(f"/api/runs/{run_id}", headers=_auth(token))
    assert run.status_code == 200
    assert run.json()["id"] == run_id


def test_auth_required_for_orchestrator_endpoints(client):
    no_auth = client.post("/api/orchestrators/next-task")
    assert no_auth.status_code == 401


def test_upload_images_from_archive(client, tmp_path, monkeypatch):
    create_task = client.post(
        "/api/tasks",
        json={
            "city": "Moscow",
            "store": "C002",
            "frequency_hours": 24,
        },
    )
    assert create_task.status_code == 201

    token = _register_orchestrator(client, name="orch-archive")
    assignment = client.post("/api/orchestrators/next-task", headers=_auth(token)).json()["assignment"]
    assert assignment is not None
    run_id = assignment["run_id"]

    archive_path = tmp_path / "bundle.tar.gz"
    image_payload = b"fake-jpeg-bytes"
    with tarfile.open(archive_path, mode="w:gz") as archive:
        member = tarfile.TarInfo(name="images/shelf.jpg")
        member.size = len(image_payload)
        archive.addfile(member, io.BytesIO(image_payload))

    def fake_upload(*, filename: str, image_bytes: bytes) -> str:
        assert filename == "shelf.jpg"
        assert image_bytes == image_payload
        return f"http://storage.local/images/{filename}"

    monkeypatch.setattr(client.app.state.image_pipeline, "_upload_to_storage", fake_upload)

    finish = client.post(
        "/api/orchestrators/results",
        headers=_auth(token),
        json={
            "run_id": run_id,
            "status": "success",
            "upload_images_from_archive": True,
            "output_gz": str(archive_path),
        },
    )
    assert finish.status_code == 200
    body = finish.json()
    assert body["processed_images"] == 1
    assert len(body["image_results"]) == 1
    assert body["image_results"][0]["uploaded_url"] == "http://storage.local/images/shelf.jpg"

    run = client.get(f"/api/runs/{run_id}", headers=_auth(token))
    assert run.status_code == 200
    run_body = run.json()
    assert run_body["image_results_json"][0]["uploaded_url"] == "http://storage.local/images/shelf.jpg"
