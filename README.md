# Distributed Task Scheduler

A small distributed job scheduler. A coordinator server holds a priority job
queue, and one or more worker processes (each running a pool of threads)
poll it for jobs, run them, and report back. If a worker dies mid-job, the
coordinator notices it stopped heartbeating and puts the job back in the
queue.

## Structure

```
coordinator/   - flask server, the priority queue, heartbeat tracking
worker/        - polls for jobs and runs them across a thread pool
client/        - small CLI for submitting jobs / checking status
```

## Running it

```bash
pip install -r requirements.txt

# terminal 1
cd coordinator && python3 app.py

# terminal 2
cd worker && python3 worker.py --id worker-A --threads 4

# terminal 3 (optional, more workers)
cd worker && python3 worker.py --id worker-B --threads 4

# terminal 4
cd client && python3 client.py flood 20
python3 client.py status
```

To see the fault-tolerance part: submit a bunch of jobs, kill a worker mid-run,
and watch the coordinator log requeue its job after the heartbeat times out.
A new/other worker will pick it up.

## Notes

- Job leasing (pop + mark as running) happens inside a single lock in
  `job_queue.py`, otherwise two workers could end up grabbing the same job.
- Priority queue is just a heap keyed on `(-priority, created_at)` so higher
  priority always wins and same-priority jobs are FIFO.
- Failed jobs get requeued up to `max_retries` times before being marked failed.
- This is at-least-once execution, not exactly-once - if a worker finishes a
  job but dies before reporting it, another worker could redo it. Fine for
  this scale, would need idempotency keys to fix properly.
- Heartbeats only prove a worker *process* is alive, not that any specific
  job is making progress - a single thread inside an otherwise-healthy
  process can hang forever. `reclaim_stale_leases()` covers this separately:
  it requeues any job whose lease (`leased_at`) has been held longer than
  `LEASE_TIMEOUT`, regardless of whether its worker is still heartbeating.
  Verified end-to-end in `test_lease_timeout.py`.

## Possible next steps
- persist the queue (sqlite) so a coordinator restart doesn't lose jobs
- push instead of poll (websockets / long-poll)
- multiple coordinator replicas for coordinator-level fault tolerance
