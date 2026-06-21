import argparse
import logging
import random
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("worker")


class Worker:
    def __init__(self, coordinator_url: str, num_threads: int, worker_id: str | None = None):
        self.coordinator_url = coordinator_url.rstrip("/")
        self.num_threads = num_threads
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"
        self._stop = threading.Event()

    def run_job(self, job: dict) -> bool:
        # placeholder work - swap this out for whatever the job should actually do
        duration = random.uniform(0.5, 2.0)
        log.info("[%s] running job %s (payload=%r, attempt %d) -- %.1fs",
                  self.worker_id, job["id"], job["payload"], job["attempts"], duration)
        time.sleep(duration)
        return random.random() > 0.15  # fail ~15% of the time so retry logic actually gets exercised

    def heartbeat_loop(self):
        while not self._stop.is_set():
            try:
                requests.post(
                    f"{self.coordinator_url}/heartbeat",
                    json={"worker_id": self.worker_id},
                    timeout=3,
                )
            except requests.RequestException as e:
                log.warning("[%s] heartbeat failed: %s", self.worker_id, e)
            time.sleep(3)

    def worker_loop(self, thread_index: int):
        while not self._stop.is_set():
            try:
                resp = requests.get(
                    f"{self.coordinator_url}/jobs/next",
                    params={"worker_id": self.worker_id},
                    timeout=5,
                )
            except requests.RequestException as e:
                log.warning("[%s-t%d] poll failed: %s", self.worker_id, thread_index, e)
                time.sleep(1)
                continue

            if resp.status_code == 204:
                time.sleep(0.5)
                continue

            job = resp.json()
            success = self.run_job(job)

            try:
                requests.post(
                    f"{self.coordinator_url}/jobs/complete",
                    json={"job_id": job["id"], "success": success},
                    timeout=5,
                )
            except requests.RequestException as e:
                log.warning("[%s-t%d] complete report failed: %s", self.worker_id, thread_index, e)

    def start(self):
        log.info("starting %s with %d threads, coordinator=%s",
                  self.worker_id, self.num_threads, self.coordinator_url)

        hb_thread = threading.Thread(target=self.heartbeat_loop, daemon=True)
        hb_thread.start()

        with ThreadPoolExecutor(max_workers=self.num_threads) as pool:
            futures = [pool.submit(self.worker_loop, i) for i in range(self.num_threads)]
            try:
                for f in futures:
                    f.result()
            except KeyboardInterrupt:
                self._stop.set()


def main():
    parser = argparse.ArgumentParser(description="task scheduler worker")
    parser.add_argument("--coordinator", default="http://localhost:8080")
    parser.add_argument("--threads", type=int, default=4)
    parser.add_argument("--id", default=None)
    args = parser.parse_args()

    worker = Worker(args.coordinator, args.threads, args.id)
    worker.start()


if __name__ == "__main__":
    main()
