import argparse
import random
import sys

import requests

DEFAULT_URL = "http://localhost:8080"


def submit(url, payload, priority, max_retries):
    resp = requests.post(f"{url}/jobs", json={
        "payload": payload,
        "priority": priority,
        "max_retries": max_retries,
    })
    resp.raise_for_status()
    print(resp.json())


def status(url):
    resp = requests.get(f"{url}/status")
    resp.raise_for_status()
    jobs = resp.json()
    if not jobs:
        print("No jobs submitted yet.")
        return
    counts = {}
    for j in jobs:
        counts[j["status"]] = counts.get(j["status"], 0) + 1
    print(f"Total jobs: {len(jobs)}  |  " + "  ".join(f"{k}={v}" for k, v in counts.items()))
    for j in sorted(jobs, key=lambda j: j["created_at"]):
        print(f"  {j['id']:<18} priority={j['priority']:<3} status={j['status']:<10} "
              f"attempts={j['attempts']} worker={j.get('worker_id') or '-'}  payload={j['payload']}")


def flood(url, n):
    sample_payloads = ["resize_image", "send_email", "transcode_video", "generate_report", "backup_db"]
    for i in range(n):
        submit(url, f"{random.choice(sample_payloads)}_{i}", random.randint(0, 9), 3)


def main():
    parser = argparse.ArgumentParser(description="task scheduler client")
    parser.add_argument("--url", default=DEFAULT_URL)
    sub = parser.add_subparsers(dest="command", required=True)

    p_submit = sub.add_parser("submit")
    p_submit.add_argument("payload")
    p_submit.add_argument("--priority", type=int, default=0)
    p_submit.add_argument("--max-retries", type=int, default=3)

    sub.add_parser("status")

    p_flood = sub.add_parser("flood")
    p_flood.add_argument("count", type=int)

    args = parser.parse_args()

    if args.command == "submit":
        submit(args.url, args.payload, args.priority, args.max_retries)
    elif args.command == "status":
        status(args.url)
    elif args.command == "flood":
        flood(args.url, args.count)


if __name__ == "__main__":
    sys.exit(main())
