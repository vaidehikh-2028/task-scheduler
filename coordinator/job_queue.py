import heapq
import itertools
import threading
import time
from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Optional


class Status(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Job:
    id: str
    payload: str
    priority: int = 0
    status: Status = Status.PENDING
    worker_id: Optional[str] = None
    attempts: int = 0
    max_retries: int = 3
    created_at: float = field(default_factory=time.time)
    leased_at: Optional[float] = None

    def to_dict(self):
        d = asdict(self)
        d["status"] = self.status.value
        return d


class JobQueue:
    def __init__(self):
        self._lock = threading.Lock()
        self._heap = []
        self._counter = itertools.count()  # avoids heapq comparing Job objects directly
        self._jobs: dict[str, Job] = {}

    def push(self, job: Job):
        with self._lock:
            self._jobs[job.id] = job
            heapq.heappush(self._heap, (-job.priority, job.created_at, next(self._counter), job.id))

    def pop(self, worker_id: str) -> Optional[Job]:
        # pop + mark running happens inside the same lock, otherwise two
        # workers could grab the same job between the pop and the status update
        with self._lock:
            while self._heap:
                _, _, _, job_id = heapq.heappop(self._heap)
                job = self._jobs.get(job_id)
                if job is None or job.status != Status.PENDING:
                    continue  # stale entry from an earlier requeue
                job.status = Status.RUNNING
                job.worker_id = worker_id
                job.leased_at = time.time()
                job.attempts += 1
                return job
            return None

    def requeue(self, job_id: str):
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return
            job.status = Status.PENDING
            job.worker_id = None
            heapq.heappush(self._heap, (-job.priority, job.created_at, next(self._counter), job.id))

    def complete(self, job_id: str, success: bool):
        with self._lock:
            job = self._jobs.get(job_id)
            if job is None:
                return
            job.status = Status.COMPLETED if success else Status.FAILED

    def get(self, job_id: str) -> Optional[Job]:
        with self._lock:
            return self._jobs.get(job_id)

    def snapshot(self) -> list[Job]:
        with self._lock:
            return list(self._jobs.values())

    def running_snapshot(self) -> list[Job]:
        with self._lock:
            return [j for j in self._jobs.values() if j.status == Status.RUNNING]
