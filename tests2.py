import httpx

BASE_URL = "http://127.0.0.1:8000"

def make_payload(i: int):
    return {
        "name": f"append line {i}",
        "command": f"echo {i} >> test.txt",
        "params": "",
        "priority": "high",
        "timeout": 30,
        "retries": 0,
        "logs": True
    }

def test_post_job():
    headers = {"Content-Type": "application/json"}
    response = httpx.post(f"{BASE_URL}/jobs/", json=make_payload(0), headers=headers)

    print("Status Code:", response.status_code)
    print("Response Body:", response.text)

    assert response.status_code == 201, f"Request failed: {response.text}"

def test_race_condition():
    for i in range(100):
        headers = {"Content-Type": "application/json"}
        response = httpx.post(f"{BASE_URL}/jobs/", json=make_payload(i), headers=headers)

        print(f"[{i}] Status Code:", response.status_code)
        print(f"[{i}] Response Body:", response.text)

        assert response.status_code == 201, f"Request failed at iteration {i}: {response.text}"


if __name__=="__main__":
    test_post_job()
    test_race_condition()
