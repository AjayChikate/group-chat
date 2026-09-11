import json
import time
import requests

def test():
    base_url = "http://127.0.0.1:5001"
    print(f"1. Testing POST /message on {base_url}...")
    
    # 1. Send first message
    msg_id = f"test_id_{int(time.time()*1000)}"
    payload = {
        "client-name": "Alice",
        "msg": "Hello, this is a test of the /message endpoint!",
        "id": msg_id,
    }
    r = requests.post(f"{base_url}/message", json=payload, timeout=5)
    print(f"POST status: {r.status_code}")
    print(f"POST body  : {r.json()}")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"
    assert r.json()["client-name"] == "Alice"
    assert r.json()["msg"] == "Hello, this is a test of the /message endpoint!"

    # 2. Duplicate send with same ID (idempotency test)
    print("\n2. Testing Deduplication (sending same msg_id again)...")
    r2 = requests.post(f"{base_url}/message", json=payload, timeout=5)
    print(f"Duplicate POST status: {r2.status_code}")
    assert r2.status_code == 200

    # 3. Form-data send
    print("\n3. Testing POST /message with form data...")
    form_payload = {
        "client-name": "Bob",
        "msg": "Form data message test",
        "id": f"bob_{int(time.time()*1000)}"
    }
    r_form = requests.post(f"{base_url}/message", data=form_payload, timeout=5)
    print(f"Form POST status: {r_form.status_code}")
    assert r_form.status_code == 200

    # 4. Fetch GET /feed
    print("\n4. Testing GET /feed...")
    r_feed = requests.get(f"{base_url}/feed?limit=5", timeout=5)
    print(f"FEED status: {r_feed.status_code}")
    feed_data = r_feed.json()
    print(f"Total messages in feed response: {len(feed_data)}")
    print(f"Sample latest message: {feed_data[-1] if feed_data else None}")
    assert r_feed.status_code == 200
    assert isinstance(feed_data, list)
    assert len(feed_data) > 0
    assert "client-name" in feed_data[0]
    assert "msg" in feed_data[0]
    assert "verified" in feed_data[0]

    print("\n>>> ALL API TESTS PASSED SUCCESSFULLY! <<<")

if __name__ == "__main__":
    test()
