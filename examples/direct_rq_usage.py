"""
Example: Direct Redis Queue usage without API.

This example shows how to enqueue jobs directly to Redis Queue
without using the REST API.
"""

import os
import sys
from pathlib import Path

import redis
from rq import Queue

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from tbt_api.worker import run_inspection_pipeline


def main():
    """Example of direct RQ usage."""

    # Connect to Redis
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

    try:
        redis_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        redis_conn.ping()
        print(f"✅ Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
    except Exception as e:
        print(f"❌ Failed to connect to Redis: {e}")
        return

    # Create queue
    queue = Queue('tbt-inspection', connection=redis_conn)

    # Example: Enqueue a job
    geojson_path = "/app/li_input/geojson/Routes2check.geojson"

    job_params = {
        "sample_id": "test-sample-001",
        "geojson_path": geojson_path,
        "provider": "Orbis",
        "competitor": "Genesis",
        "product": "latest",
        "mapdate": "2026-03-03"
    }

    print("\n📤 Enqueueing job...")
    print(f"   Sample ID: {job_params['sample_id']}")
    print(f"   Provider: {job_params['provider']}")
    print(f"   Competitor: {job_params['competitor']}")

    job = queue.enqueue(
        run_inspection_pipeline,
        kwargs=job_params,
        job_timeout='2h',
        result_ttl=86400,
        failure_ttl=86400
    )

    print(f"\n✅ Job enqueued!")
    print(f"   Job ID: {job.id}")
    print(f"   Status: {job.get_status()}")

    # Wait for completion (optional)
    print("\n⏳ Waiting for job to complete...")

    import time
    while not job.is_finished and not job.is_failed:
        time.sleep(5)
        job.refresh()
        print(f"   Status: {job.get_status()}")

    if job.is_finished:
        print(f"\n✅ Job completed successfully!")
        print(f"   Result: {job.result}")
    elif job.is_failed:
        print(f"\n❌ Job failed!")
        print(f"   Error: {job.exc_info}")


if __name__ == "__main__":
    main()

