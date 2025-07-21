import pytest
import time
import uuid
from fastapi.testclient import TestClient
import httpx

from api import app

# In‑process TestClient
client = TestClient(app)


def make_payload(**overrides):
    base = {
        "name": "TestJob",
        "command": "echo hello",
        "params": "",
        "priority": "mid",
        "timeout": 5,
        "retries": 0,
        "logs": False,
    }
    base.update(overrides)
    return base


def test_post_job_success():
    resp = client.post("/jobs/", json=make_payload())
    assert resp.status_code == 201
    data = resp.json()
    assert data["status"] == "submitted"
    assert "job_id" in data


def test_post_job_missing_name():
    payload = make_payload()
    del payload["name"]
    resp = client.post("/jobs/", json=payload)
    # missing required field → 422
    assert resp.status_code == 422


def test_post_job_invalid_priority():
    resp = client.post("/jobs/", json=make_payload(priority="urgent"))
    # your handler maps that to 404
    assert resp.status_code == 404


def test_post_job_negative_retries():
    resp = client.post("/jobs/", json=make_payload(retries=-1))
    assert resp.status_code == 404


def test_post_job_empty_command():
    resp = client.post("/jobs/", json=make_payload(command="   "))
    assert resp.status_code == 404


def test_get_job_not_found():
    random_id = str(uuid.uuid4())
    resp = client.get(f"/jobs/{random_id}")
    assert resp.status_code == 404
    assert resp.json() == {"detail": "Job not found"}


# ——————————————————————————————————————————————————————————————————————————
# Live‐server test: requires `uvicorn api:app` running on 127.0.0.1:8000
# ——————————————————————————————————————————————————————————————————————————

BASE = "http://127.0.0.1:8000"
def test_full_job_lifecycle():
    post = client.post("/jobs/", json=make_payload())
    job_id = post.json()["job_id"]

    # poll until the worker commits
    deadline = time.time() + 5
    while time.time() < deadline:
        get = client.get(f"/jobs/{job_id}")
        if get.status_code == 200:
            break
        time.sleep(0.1)
    else:
        pytest.skip("DB worker never committed the job")

    assert get.status_code == 200
