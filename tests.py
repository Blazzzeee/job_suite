

from fastapi.testclient import TestClient
from api import app  # import your FastAPI app

client = TestClient(app)

valid_job = {
    "job_id": "job123",
    "command": "echo Hello",
    "retries": 3,
    "priority": "mid"
}



def test_post_job_success():
    response = client.post("/post_jobs", json=valid_job)
    assert response.status_code == 200
    assert response.json()["message"] == "Job received"



def test_post_job_missing_field():
    invalid_job = valid_job.copy()
    del invalid_job["job_id"]
    response = client.post("/post_jobs", json=invalid_job)
    assert response.status_code == 422  # Unprocessable Entity


def test_post_job_invalid_priority():
    invalid_job = valid_job.copy()
    invalid_job["priority"] = "urgent"  # not 'low', 'mid', 'high'
    response = client.post("/post_jobs", json=invalid_job)
    assert response.status_code == 400
    assert "Invalid priority" in response.json()["detail"]


def test_post_job_negative_retries():
    invalid_job = valid_job.copy()
    invalid_job["retries"] = -1
    response = client.post("/post_jobs", json=invalid_job)
    assert response.status_code == 422


def test_post_job_empty_command():
    invalid_job = valid_job.copy()
    invalid_job["command"] = ""
    response = client.post("/post_jobs", json=invalid_job)
    assert response.status_code == 422
