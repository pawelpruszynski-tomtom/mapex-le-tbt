"""
Example: Using TbT Inspection API Client.

This example demonstrates how to use the Python client to submit
inspection jobs and track their progress.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from tbt_api.client import TbtInspectionClient


def example_submit_and_wait():
    """Example: Submit job and wait for completion."""

    # Create client
    client = TbtInspectionClient(base_url="http://localhost:8000")

    # Check API health
    health = client.health_check()
    print(f"API Status: {health['status']}")
    print(f"Redis: {health['redis']}")

    # Submit inspection job
    geojson_file = "li_input/geojson/Routes2check.geojson"

    print(f"\n📤 Submitting inspection job...")

    result = client.submit_and_wait(
        geojson_file=geojson_file,
        provider="Orbis",
        competitor="Genesis",
        product="latest",
        mapdate="2026-03-03",
        check_interval=10,  # Check every 10 seconds
        max_wait=7200,      # Wait max 2 hours
        verbose=True
    )

    if result['status'] == 'finished':
        print("\n✅ Inspection completed successfully!")
        print(f"\nResults:")
        print(f"  Sample ID: {result['result']['sample_id']}")
        print(f"  Duration: {result['result']['duration_seconds']:.2f} seconds")
        print(f"  Output files: {result['result']['output_files']}")
    else:
        print(f"\n❌ Inspection failed!")
        print(f"  Error: {result.get('error')}")


def example_submit_and_track_separately():
    """Example: Submit job and track separately."""

    # Create client
    client = TbtInspectionClient(base_url="http://localhost:8000")

    # Submit job
    submission = client.submit_inspection(
        geojson_file="li_input/geojson/Routes2check.geojson",
        provider="Orbis",
        competitor="Genesis"
    )

    job_id = submission['job_id']
    print(f"Job submitted: {job_id}")

    # Later... check status
    status = client.get_job_status(job_id)
    print(f"Job status: {status['status']}")

    # If needed, cancel
    if status['status'] in ['queued', 'started']:
        # cancel_response = client.cancel_job(job_id)
        # print(f"Job canceled: {cancel_response}")
        pass


def example_queue_stats():
    """Example: Get queue statistics."""

    client = TbtInspectionClient(base_url="http://localhost:8000")

    stats = client.get_queue_stats()
    print("\n📊 Queue Statistics:")
    print(f"  Queued jobs: {stats['queued_jobs']}")
    print(f"  Running jobs: {stats['started_jobs']}")
    print(f"  Finished jobs: {stats['finished_jobs']}")
    print(f"  Failed jobs: {stats['failed_jobs']}")
    print(f"  Workers: {stats['workers']}")


def example_batch_submission():
    """Example: Submit multiple jobs."""

    client = TbtInspectionClient(base_url="http://localhost:8000")

    # List of GeoJSON files to process
    geojson_files = [
        "routes_batch_1.geojson",
        "routes_batch_2.geojson",
        "routes_batch_3.geojson"
    ]

    job_ids = []

    for geojson_file in geojson_files:
        if not Path(geojson_file).exists():
            print(f"⚠️  Skipping {geojson_file} (not found)")
            continue

        submission = client.submit_inspection(
            geojson_file=geojson_file,
            provider="Orbis",
            competitor="Genesis"
        )

        job_ids.append(submission['job_id'])
        print(f"✅ Submitted: {submission['job_id']} ({geojson_file})")

    print(f"\n📊 Submitted {len(job_ids)} jobs")

    # Track all jobs
    print("\n⏳ Waiting for all jobs to complete...")

    for job_id in job_ids:
        try:
            result = client.wait_for_completion(job_id, verbose=False)
            print(f"✅ {job_id}: {result['status']}")
        except Exception as e:
            print(f"❌ {job_id}: {e}")


if __name__ == "__main__":
    # Run example
    example_submit_and_wait()

    # Uncomment to run other examples:
    # example_submit_and_track_separately()
    # example_queue_stats()
    # example_batch_submission()

