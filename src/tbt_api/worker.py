"""
RQ Worker functions for TbT Inspection Pipeline.

This module contains the worker function that processes inspection jobs.
"""

import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path

# Add project to path
sys.path.insert(0, '/app/src')

from dotenv import load_dotenv

# Load environment variables
load_dotenv('/app/.env')

logger = logging.getLogger(__name__)


def run_inspection_pipeline(
    pipeline_id: str,
    provider: str = "Orbis",
    competitor: str = "Genesis",
    product: str = "latest",
    mapdate: str = "2026-03-03",
    country: str = None,
    endpoint: str = None,
    competitor_endpoint: str = None,
    ignore_previous_inspections: bool = True,
    avoid_duplicates: bool = False
) -> dict:
    """
    Run the TbT inspection pipeline with data from database.

    This function is executed by RQ workers.

    Args:
        pipeline_id: UUID of the pipeline in database (also used as sample_id)
        provider: Provider under test
        competitor: Reference competitor
        product: Map product version
        mapdate: Map date
        country: Country code (optional)
        endpoint: Custom provider endpoint (optional)
        competitor_endpoint: Custom competitor endpoint (optional)
        ignore_previous_inspections: Whether to ignore previous inspections
        avoid_duplicates: Whether to avoid duplicate processing

    Returns:
        dict: Result summary with paths to output files
    """
    # Use pipeline_id as sample_id (they are the same in this system)
    sample_id = pipeline_id

    start_time = datetime.utcnow()
    logger.info(f"Starting inspection pipeline for sample_id/pipeline_id: {sample_id}")

    try:
        # Import Kedro components
        from kedro.framework.session import KedroSession
        from kedro.framework.startup import bootstrap_project

        # Bootstrap Kedro project
        project_path = Path("/app")
        bootstrap_project(project_path)

        # Update parameters with pipeline_id
        params_to_update = {
            "tbt_options": {
                "sample_id": sample_id,
                "pipeline_id": pipeline_id,
                "provider": provider,
                "competitor": competitor,
                "product": product,
                "mapdate": mapdate,
                "ignore_previous_inspections": ignore_previous_inspections,
                "avoid_duplicates": avoid_duplicates,
                "skip_cleanup": False,
                "error_classification_mode": False
            }
        }

        if endpoint:
            params_to_update["tbt_options"]["endpoint"] = endpoint
        if competitor_endpoint:
            params_to_update["tbt_options"]["competitor_endpoint"] = competitor_endpoint
        if country:
            params_to_update["tbt_options"]["country"] = country

        # Run the pipeline
        with KedroSession.create(
            project_path=project_path,
            extra_params=params_to_update
        ) as session:
            logger.info(f"Running pipeline tbt_inspection for sample {sample_id}")

            # Run the full inspection pipeline
            session.run(pipeline_name="tbt_inspection")

            logger.info(f"Pipeline completed successfully for sample {sample_id}")

        # Collect output files
        output_dir = project_path / "output"
        data_dir = project_path / "data" / "tbt" / "inspection"

        output_files = {
            "csv": {},
            "parquet": {},
            "database": "exported"
        }

        # CSV files
        csv_files = [
            "inspection_routes.csv",
            "inspection_critical_sections.csv",
            "critical_sections_with_mcp_feedback.csv",
            "error_logs.csv",
            "inspection_metadata.csv"
        ]

        for csv_file in csv_files:
            csv_path = output_dir / csv_file
            if csv_path.exists():
                output_files["csv"][csv_file] = str(csv_path)

        # Parquet directories
        parquet_dirs = [
            "inspection_routes",
            "inspection_critical_sections",
            "critical_sections_with_mcp_feedback",
            "error_logs",
            "inspection_metadata"
        ]

        for parquet_dir in parquet_dirs:
            parquet_path = data_dir / parquet_dir
            if parquet_path.exists():
                output_files["parquet"][parquet_dir] = str(parquet_path)

        # Calculate duration
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()

        result = {
            "status": "success",
            "sample_id": sample_id,
            "provider": provider,
            "competitor": competitor,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "output_files": output_files
        }

        logger.info(f"Inspection completed in {duration:.2f} seconds")
        return result

    except Exception as e:
        logger.error(f"Pipeline failed for sample {sample_id}: {str(e)}", exc_info=True)

        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds()

        return {
            "status": "failed",
            "sample_id": sample_id,
            "error": str(e),
            "error_type": type(e).__name__,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration
        }

