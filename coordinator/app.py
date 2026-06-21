import itertools
import logging
import threading
import time

from flask import Flask, jsonify, request

from job_queue import Job, JobQueue, Status
from heartbeat import HeartbeatTracker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("coordinator")

app = Flask(__name__)
queue = JobQueue()
heartbeats = HeartbeatTracker(timeout=10.0)
_id_counter = itertools.count(1)
_id_lock = threading.Lock()


def new_job_id() -> str:
    with _id_lock:
        n = next(_id_counter)
    return f"{int(time.time())}-{n}"


@app.post("/jobs")
def submit_job():
    body = request.get_json(force=True) or {}
    payload = body.get("payload", "")
    priority = int(body.get("priority", 0))
    max_retries = int(body.get("max_retries", 3))

    job = Job(id=new_job_id(), payload=payload, priority=priority, max_retries=max_retries)
    queue.push(job)
    log.info("submitted job %s (priority=%d)", job.id, priority)
    return jsonify(job.to_dict()), 201


@app.get("/jobs/next")
def next_job():
    worker_id = request.args.get("worker_id")
    if not worker_id:
        return jsonify({"error": "worker_id required"}), 400

    heartbeats.beat(worker_id)
    job = queue.pop(worker_id)
    if job is None:
        return "", 204
    log.info("leased job %s -> worker %s (attempt %d)", job.id, worker_id, job.attempts)
    return jsonify(job.to_dict())


@app.post("/heartbeat")
def heartbeat():
    body = request.get_json(force=True) or {}
    worker_id = body.get("worker_id")
    if not worker_id:
        return jsonify({"error": "worker_id required"}), 400
    heartbeats.beat(worker_id)
    return "", 200


@app.post("/jobs/complete")
def complete_job():
    body = request.get_json(force=True) or {}
    job_id = body.get("job_id")
    success = bool(body.get("success", False))

    if not success:
        job = queue.get(job_id)
        if job is not None and job.attempts <= job.max_retries:
            log.warning("job %s failed, requeuing (attempt %d/%d)", job_id, job.attempts, job.max_retries)
            queue.requeue(job_id)
            return jsonify(job.to_dict())

    queue.complete(job_id, success)
    log.info("job %s finished, success=%s", job_id, success)
    return "", 200


@app.get("/status")
def status():
    return jsonify([j.to_dict() for j in queue.snapshot()])


def monitor_dead_workers(interval: float = 3.0):
    while True:
        time.sleep(interval)
        for job in queue.running_snapshot():
            if job.worker_id and not heartbeats.is_alive(job.worker_id):
                log.warning("worker %s presumed dead, requeuing job %s", job.worker_id, job.id)
                queue.requeue(job.id)


def main():
    monitor_thread = threading.Thread(target=monitor_dead_workers, daemon=True)
    monitor_thread.start()
    log.info("coordinator starting on http://0.0.0.0:8080")
    app.run(host="0.0.0.0", port=8080, threaded=True)


if __name__ == "__main__":
    main()
