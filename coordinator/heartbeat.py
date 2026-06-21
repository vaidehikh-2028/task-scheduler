import threading
import time


class HeartbeatTracker:
    def __init__(self, timeout: float = 10.0):
        self._lock = threading.Lock()
        self._last_seen: dict[str, float] = {}
        self.timeout = timeout

    def beat(self, worker_id: str):
        with self._lock:
            self._last_seen[worker_id] = time.time()

    def is_alive(self, worker_id: str) -> bool:
        with self._lock:
            last = self._last_seen.get(worker_id)
            if last is None:
                return False
            return (time.time() - last) < self.timeout
