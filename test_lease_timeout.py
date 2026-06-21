"""
Verifies the lease-timeout fix: a worker that keeps heartbeating normally
(so it looks alive) but whose job-thread is stuck (never calls /jobs/complete)
should still have its job reclaimed after LEASE_TIMEOUT, and picked up by a
different worker.
"""
import threading
import time
import requests

BASE = "http://127.0.0.1:8080"


def stuck_worker_heartbeat_loop(worker_id, stop_event):
    while not stop_event.is_set():
        requests.post(f"{BASE}/heartbeat", json={"worker_id": worker_id}, timeout=3)
        time.sleep(2)


def main():
    stop_event = threading.Event()

    # 1. Submit one job.
    resp = requests.post(f"{BASE}/jobs", json={"payload": "stuck_test_job", "priority": 5})
    job_id = resp.json()["id"]
    print(f"submitted job {job_id}")

    # 2. "Stuck worker" leases it...
    stuck_worker_id = "stuck-worker"
    resp = requests.get(f"{BASE}/jobs/next", params={"worker_id": stuck_worker_id})
    leased = resp.json()
    assert leased["id"] == job_id, "expected the stuck worker to get our job"
    print(f"stuck-worker leased job {job_id} -- now going 'stuck' (never completes it)")

    # ...but keeps heartbeating forever, simulating a process that's alive
    # while one thread inside it is hung.
    hb_thread = threading.Thread(target=stuck_worker_heartbeat_loop, args=(stuck_worker_id, stop_event), daemon=True)
    hb_thread.start()

    # 3. Poll /status every 2s and report what we see.
    for i in range(12):  # ~24s, enough to cross the 15s LEASE_TIMEOUT
        time.sleep(2)
        resp = requests.get(f"{BASE}/status")
        jobs = {j["id"]: j for j in resp.json()}
        j = jobs[job_id]
        print(f"t={2*(i+1):>3}s  status={j['status']:<8} worker={j['worker_id']}  attempts={j['attempts']}")

        if j["status"] == "pending" and j["worker_id"] is None:
            print("\n>>> Job was reclaimed from the stuck worker. Now a healthy worker grabs it:")
            resp2 = requests.get(f"{BASE}/jobs/next", params={"worker_id": "healthy-worker"})
            taken = resp2.json()
            print(f">>> healthy-worker successfully leased job {taken['id']} (attempt {taken['attempts']})")
            requests.post(f"{BASE}/jobs/complete", json={"job_id": job_id, "success": True})
            print(">>> healthy-worker reported it complete.")
            stop_event.set()
            return

    stop_event.set()
    print("\n!! Job was never reclaimed within the test window -- something's wrong.")


if __name__ == "__main__":
    main()
