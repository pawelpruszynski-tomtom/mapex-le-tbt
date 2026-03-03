"""
Python client for TbT Inspection API.

This module provides a simple client for interacting with the TbT Inspection API.
"""

import time
from pathlib import Path
from typing import Optional, Dict, Any

import requests


class TbtInspectionClient:
    """Client for TbT Inspection API."""

    def __init__(self, base_url: str = "http://localhost:8000"):
        """
        Initialize the client.

        Args:
            base_url: Base URL of the API
        """
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()

    def health_check(self) -> Dict[str, Any]:
        """
        Check API health.

        Returns:
            dict: Health status
        """
        response = self.session.get(f"{self.base_url}/health")
        response.raise_for_status()
        return response.json()

    def submit_inspection(
        self,
        geojson_file: str,
        provider: str = "Orbis",
        competitor: str = "Genesis",
        product: str = "latest",
        mapdate: str = "2026-03-03",
        country: Optional[str] = None,
        endpoint: Optional[str] = None,
        competitor_endpoint: Optional[str] = None,
        ignore_previous_inspections: bool = True,
        avoid_duplicates: bool = False
    ) -> Dict[str, Any]:
        """
        Submit an inspection job.

        Args:
            geojson_file: Path to GeoJSON file
            provider: Provider name
            competitor: Competitor name
            product: Product version
            mapdate: Map date (YYYY-MM-DD)
            country: Country code (optional)
            endpoint: Custom provider endpoint (optional)
            competitor_endpoint: Custom competitor endpoint (optional)
            ignore_previous_inspections: Whether to ignore previous inspections
            avoid_duplicates: Whether to avoid duplicates

        Returns:
            dict: Job submission response with job_id

        Raises:
            FileNotFoundError: If GeoJSON file not found
            requests.HTTPError: If API request fails
        """
        geojson_path = Path(geojson_file)
        if not geojson_path.exists():
            raise FileNotFoundError(f"GeoJSON file not found: {geojson_file}")

        # Prepare request
        with open(geojson_path, 'rb') as f:
            files = {'geojson_file': (geojson_path.name, f, 'application/json')}
            data = {
                'provider': provider,
                'competitor': competitor,
                'product': product,
                'mapdate': mapdate,
                'ignore_previous_inspections': ignore_previous_inspections,
                'avoid_duplicates': avoid_duplicates
            }

            if country:
                data['country'] = country
            if endpoint:
                data['endpoint'] = endpoint
            if competitor_endpoint:
                data['competitor_endpoint'] = competitor_endpoint

            response = self.session.post(
                f"{self.base_url}/api/v1/inspection/submit",
                files=files,
                data=data
            )

        response.raise_for_status()
        return response.json()

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """
        Get job status.

        Args:
            job_id: Job ID

        Returns:
            dict: Job status

        Raises:
            requests.HTTPError: If API request fails
        """
        response = self.session.get(
            f"{self.base_url}/api/v1/inspection/status/{job_id}"
        )
        response.raise_for_status()
        return response.json()

    def cancel_job(self, job_id: str) -> Dict[str, Any]:
        """
        Cancel a job.

        Args:
            job_id: Job ID

        Returns:
            dict: Cancellation response

        Raises:
            requests.HTTPError: If API request fails
        """
        response = self.session.delete(
            f"{self.base_url}/api/v1/inspection/cancel/{job_id}"
        )
        response.raise_for_status()
        return response.json()

    def get_queue_stats(self) -> Dict[str, Any]:
        """
        Get queue statistics.

        Returns:
            dict: Queue statistics

        Raises:
            requests.HTTPError: If API request fails
        """
        response = self.session.get(
            f"{self.base_url}/api/v1/inspection/queue/stats"
        )
        response.raise_for_status()
        return response.json()

    def wait_for_completion(
        self,
        job_id: str,
        check_interval: int = 10,
        max_wait: int = 7200,
        verbose: bool = True
    ) -> Dict[str, Any]:
        """
        Wait for job to complete.

        Args:
            job_id: Job ID
            check_interval: Seconds between status checks
            max_wait: Maximum seconds to wait
            verbose: Whether to print status updates

        Returns:
            dict: Final job status

        Raises:
            TimeoutError: If job doesn't complete within max_wait
        """
        start_time = time.time()
        last_status = None

        while time.time() - start_time < max_wait:
            status = self.get_job_status(job_id)
            current_status = status['status']

            # Print status if changed
            if verbose and current_status != last_status:
                elapsed = int(time.time() - start_time)
                print(f"[{elapsed}s] Status: {current_status}")

                if status.get('progress'):
                    print(f"  Progress: {status['progress']}%")

            last_status = current_status

            # Check if finished
            if current_status in ['finished', 'failed', 'canceled']:
                return status

            # Wait before next check
            time.sleep(check_interval)

        raise TimeoutError(f"Job {job_id} did not complete within {max_wait} seconds")

    def submit_and_wait(
        self,
        geojson_file: str,
        provider: str = "Orbis",
        competitor: str = "Genesis",
        product: str = "latest",
        mapdate: str = "2026-03-03",
        check_interval: int = 10,
        max_wait: int = 7200,
        verbose: bool = True,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Submit job and wait for completion.

        This is a convenience method that combines submit_inspection and wait_for_completion.

        Args:
            geojson_file: Path to GeoJSON file
            provider: Provider name
            competitor: Competitor name
            product: Product version
            mapdate: Map date
            check_interval: Seconds between status checks
            max_wait: Maximum seconds to wait
            verbose: Whether to print status updates
            **kwargs: Additional arguments passed to submit_inspection

        Returns:
            dict: Final job status with results

        Raises:
            TimeoutError: If job doesn't complete within max_wait
        """
        # Submit job
        submission = self.submit_inspection(
            geojson_file=geojson_file,
            provider=provider,
            competitor=competitor,
            product=product,
            mapdate=mapdate,
            **kwargs
        )

        if verbose:
            print(f"Job submitted: {submission['job_id']}")
            print(f"Sample ID: {submission['sample_id']}")

        # Wait for completion
        return self.wait_for_completion(
            job_id=submission['job_id'],
            check_interval=check_interval,
            max_wait=max_wait,
            verbose=verbose
        )


# Example usage
if __name__ == "__main__":
    import sys

    # Create client
    client = TbtInspectionClient()

    # Check health
    health = client.health_check()
    print(f"API Status: {health['status']}")

    # Get GeoJSON file from command line
    if len(sys.argv) < 2:
        print("Usage: python tbt_client.py <geojson_file>")
        sys.exit(1)

    geojson_file = sys.argv[1]

    # Submit and wait for completion
    try:
        result = client.submit_and_wait(
            geojson_file=geojson_file,
            provider="Orbis",
            competitor="Genesis",
            verbose=True
        )

        if result['status'] == 'finished':
            print("\n✅ Inspection completed successfully!")
            print(f"Results: {result.get('result')}")
        else:
            print(f"\n❌ Inspection failed: {result.get('error')}")
            sys.exit(1)

    except TimeoutError as e:
        print(f"\n⏱️ {e}")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)

