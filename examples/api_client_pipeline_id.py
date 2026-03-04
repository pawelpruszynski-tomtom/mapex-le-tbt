#!/usr/bin/env python3
"""
Example: Submit inspection job using pipeline_id from database.

This script demonstrates the new way of submitting inspection jobs
using pipeline_id instead of uploading GeoJSON files.
"""

import requests
import time
import sys


API_BASE_URL = "http://localhost:8000"


def submit_job_with_pipeline_id(pipeline_id: str, provider: str = "Orbis", competitor: str = "Genesis"):
    """
    Submit an inspection job with pipeline_id.

    Args:
        pipeline_id: UUID of the pipeline in database
        provider: Provider name
        competitor: Competitor name

    Returns:
        dict: Job information
    """
    url = f"{API_BASE_URL}/api/v1/inspection/submit"

    data = {
        "pipeline_id": pipeline_id,
        "provider": provider,
        "competitor": competitor,
        "product": "latest",
        "mapdate": "2026-03-03"
    }

    print(f"📤 Submitting job for pipeline_id: {pipeline_id}")
    response = requests.post(url, data=data)

    if response.status_code == 200:
        result = response.json()
        print(f"✅ Job submitted successfully!")
        print(f"   Job ID: {result['job_id']}")
        print(f"   Sample ID: {result['sample_id']}")
        return result
    else:
        print(f"❌ Failed to submit job: {response.status_code}")
        print(f"   Error: {response.text}")
        return None


def check_job_status(job_id: str):
    """Check the status of a job."""
    url = f"{API_BASE_URL}/api/v1/inspection/status/{job_id}"

    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    return None


def wait_for_completion(job_id: str, timeout: int = 3600):
    """
    Wait for job to complete.

    Args:
        job_id: Job ID
        timeout: Maximum seconds to wait
    """
    print(f"\n⏳ Waiting for job {job_id} to complete...")

    start_time = time.time()
    while time.time() - start_time < timeout:
        status = check_job_status(job_id)

        if not status:
            print("❌ Could not get job status")
            return False

        current_status = status['status']
        print(f"   Status: {current_status}")

        if current_status == 'finished':
            print(f"\n✅ Job completed successfully!")
            if status.get('result'):
                print(f"\n📊 Results:")
                print(f"   Duration: {status['result'].get('duration_seconds', 'N/A')} seconds")
                print(f"   Output files: {status['result'].get('output_files', {})}")
            return True

        if current_status == 'failed':
            print(f"\n❌ Job failed!")
            if status.get('error'):
                print(f"   Error: {status['error']}")
            return False

        time.sleep(10)

    print(f"\n⏱️ Timeout: Job did not complete within {timeout} seconds")
    return False


def main():
    """Main function."""
    if len(sys.argv) < 2:
        print("Usage: python api_client_pipeline_id.py <pipeline_id> [provider] [competitor]")
        print("\nExample:")
        print("  python api_client_pipeline_id.py b294bb07-b9d6-4e6f-8100-b909fe6227df")
        print("  python api_client_pipeline_id.py b294bb07-b9d6-4e6f-8100-b909fe6227df Orbis Genesis")
        sys.exit(1)

    pipeline_id = sys.argv[1]
    provider = sys.argv[2] if len(sys.argv) > 2 else "Orbis"
    competitor = sys.argv[3] if len(sys.argv) > 3 else "Genesis"

    # Submit job
    result = submit_job_with_pipeline_id(pipeline_id, provider, competitor)

    if not result:
        sys.exit(1)

    job_id = result['job_id']

    # Wait for completion
    success = wait_for_completion(job_id)

    if success:
        print("\n🎉 Inspection completed successfully!")
        sys.exit(0)
    else:
        print("\n❌ Inspection failed")
        sys.exit(1)


if __name__ == "__main__":
    main()

