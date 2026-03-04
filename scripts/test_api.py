#!/usr/bin/env python3
"""
Test script for TbT Inspection API.

This script demonstrates how to submit a GeoJSON file for inspection
and track the job status.
"""

import requests
import time
import json
import sys
from pathlib import Path


# API Configuration
API_BASE_URL = "http://localhost:8000"
API_SUBMIT_URL = f"{API_BASE_URL}/api/v1/inspection/submit"
API_STATUS_URL = f"{API_BASE_URL}/api/v1/inspection/status"


def check_api_health():
    """Check if API is running."""
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=5)
        if response.status_code == 200:
            print("✅ API is healthy")
            return True
        else:
            print(f"⚠️  API returned status {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ API is not accessible: {e}")
        return False


def submit_inspection_job(
    pipeline_id: str,
    provider: str = "Orbis",
    competitor: str = "Genesis",
    product: str = "latest",
    mapdate: str = "2026-03-03"
):
    """
    Submit a pipeline_id for inspection.

    Args:
        pipeline_id: UUID of the pipeline in database
        provider: Provider name
        competitor: Competitor name
        product: Product version
        mapdate: Map date

    Returns:
        dict: Response with job_id
    """
    print(f"\n📤 Submitting inspection job...")
    print(f"   Pipeline ID: {pipeline_id}")
    print(f"   Provider: {provider}")
    print(f"   Competitor: {competitor}")

    # Prepare request
    data = {
        'pipeline_id': pipeline_id,
        'provider': provider,
        'competitor': competitor,
        'product': product,
        'mapdate': mapdate
    }

    try:
        response = requests.post(API_SUBMIT_URL, data=data, timeout=30)

        if response.status_code == 200:
            result = response.json()
            print(f"\n✅ Job submitted successfully!")
            print(f"   Job ID: {result['job_id']}")
            print(f"   Sample ID: {result['sample_id']}")
            print(f"   Status: {result['status']}")
            return result
        else:
            print(f"❌ Failed to submit job: {response.status_code}")
            print(f"   Error: {response.text}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
        return None


def get_job_status(job_id: str):
    """
    Get the status of a job.

    Args:
        job_id: Job ID

    Returns:
        dict: Job status
    """
    try:
        response = requests.get(f"{API_STATUS_URL}/{job_id}", timeout=10)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ Failed to get status: {response.status_code}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
        return None


def wait_for_job_completion(job_id: str, check_interval: int = 10, max_wait: int = 7200):
    """
    Wait for job to complete, printing status updates.

    Args:
        job_id: Job ID
        check_interval: Seconds between status checks
        max_wait: Maximum seconds to wait

    Returns:
        dict: Final job status
    """
    print(f"\n⏳ Waiting for job {job_id} to complete...")
    print(f"   (Checking every {check_interval} seconds, max wait {max_wait} seconds)")

    start_time = time.time()
    last_status = None

    while time.time() - start_time < max_wait:
        status = get_job_status(job_id)

        if not status:
            time.sleep(check_interval)
            continue

        current_status = status['status']

        # Print status if changed
        if current_status != last_status:
            elapsed = int(time.time() - start_time)
            print(f"\n[{elapsed}s] Status: {current_status}")

            if 'progress' in status and status['progress']:
                print(f"   Progress: {status['progress']}%")

        last_status = current_status

        # Check if finished
        if current_status == 'finished':
            print(f"\n✅ Job completed successfully!")
            if status.get('result'):
                print(f"\n📊 Results:")
                print(json.dumps(status['result'], indent=2))
            return status

        # Check if failed
        if current_status == 'failed':
            print(f"\n❌ Job failed!")
            if status.get('error'):
                print(f"   Error: {status['error']}")
            return status

        # Wait before next check
        time.sleep(check_interval)

    print(f"\n⏱️  Timeout: Job did not complete within {max_wait} seconds")
    return get_job_status(job_id)


def main():
    """Main test function."""
    print("=" * 70)
    print("TbT Inspection API Test")
    print("=" * 70)

    # Check if API is running
    if not check_api_health():
        print("\n💡 Make sure the API is running:")
        print("   docker-compose up -d")
        sys.exit(1)

    # Get pipeline_id from command line
    if len(sys.argv) > 1:
        pipeline_id = sys.argv[1]
    else:
        print(f"\n❌ No pipeline_id provided")
        print(f"\nUsage:")
        print(f"   python {sys.argv[0]} <pipeline_id>")
        print(f"\nExample:")
        print(f"   python {sys.argv[0]} b294bb07-b9d6-4e6f-8100-b909fe6227df")
        sys.exit(1)

    # Submit job
    result = submit_inspection_job(
        pipeline_id,
        provider="Orbis",
        competitor="Genesis"
    )

    if not result:
        sys.exit(1)

    job_id = result['job_id']

    # Wait for completion
    final_status = wait_for_job_completion(job_id, check_interval=10)

    if final_status and final_status['status'] == 'finished':
        print("\n🎉 Inspection completed successfully!")
        sys.exit(0)
    else:
        print("\n❌ Inspection failed or timed out")
        sys.exit(1)


if __name__ == "__main__":
    main()

