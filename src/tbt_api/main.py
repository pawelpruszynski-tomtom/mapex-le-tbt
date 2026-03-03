"""
FastAPI application for TbT Inspection Pipeline.

This API allows submitting GeoJSON files for inspection processing
via Redis Queue.
"""

import logging
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks, Form
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
import redis
from rq import Queue
from rq.job import Job

from .worker import run_inspection_pipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="TbT Inspection API",
    description="API for running Turn-by-Turn inspection pipeline with GeoJSON input",
    version="1.0.0"
)

# Redis connection
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

try:
    redis_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    redis_conn.ping()
    logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
except Exception as e:
    logger.error(f"Failed to connect to Redis: {e}")
    redis_conn = None

# RQ Queue
inspection_queue = Queue('tbt-inspection', connection=redis_conn) if redis_conn else None


# Pydantic models
class JobSubmitResponse(BaseModel):
    job_id: str
    status: str
    message: str
    sample_id: str
    geojson_path: str


class JobStatusResponse(BaseModel):
    job_id: str
    status: str
    result: Optional[dict] = None
    error: Optional[str] = None
    progress: Optional[float] = None
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    ended_at: Optional[str] = None


class InspectionRequest(BaseModel):
    provider: str = Field(default="Orbis", description="Provider under test")
    competitor: str = Field(default="Genesis", description="Reference competitor")
    product: str = Field(default="latest", description="Map product version")
    mapdate: str = Field(default="2026-03-03", description="Map date (YYYY-MM-DD)")
    country: Optional[str] = Field(default=None, description="Country code")
    endpoint: Optional[str] = Field(default=None, description="Custom provider endpoint")
    competitor_endpoint: Optional[str] = Field(default=None, description="Custom competitor endpoint")
    ignore_previous_inspections: bool = Field(default=True)
    avoid_duplicates: bool = Field(default=False)


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "name": "TbT Inspection API",
        "version": "1.0.0",
        "status": "running",
        "redis_connected": redis_conn is not None,
        "endpoints": {
            "submit": "/api/v1/inspection/submit",
            "status": "/api/v1/inspection/status/{job_id}",
            "health": "/health",
            "docs": "/docs"
        }
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    redis_ok = False
    if redis_conn:
        try:
            redis_conn.ping()
            redis_ok = True
        except:
            pass

    return {
        "status": "healthy" if redis_ok else "degraded",
        "redis": "connected" if redis_ok else "disconnected",
        "timestamp": datetime.utcnow().isoformat()
    }


@app.post("/api/v1/inspection/submit", response_model=JobSubmitResponse)
async def submit_inspection(
    geojson_file: UploadFile = File(..., description="GeoJSON file with routes"),
    provider: str = Form(default="Orbis"),
    competitor: str = Form(default="Genesis"),
    product: str = Form(default="latest"),
    mapdate: str = Form(default="2026-03-03"),
    country: Optional[str] = Form(default=None),
    endpoint: Optional[str] = Form(default=None),
    competitor_endpoint: Optional[str] = Form(default=None),
    ignore_previous_inspections: bool = Form(default=True),
    avoid_duplicates: bool = Form(default=False)
):
    """
    Submit a new inspection job with GeoJSON file.

    The GeoJSON file should contain routes to inspect.
    Returns a job_id that can be used to track the job status.
    """
    if not redis_conn or not inspection_queue:
        raise HTTPException(status_code=503, detail="Redis queue not available")

    # Validate file type
    if not geojson_file.filename.endswith(('.geojson', '.json')):
        raise HTTPException(status_code=400, detail="File must be a GeoJSON (.geojson or .json)")

    # Generate sample_id
    sample_id = str(uuid.uuid4())

    # Create upload directory
    upload_dir = Path("/app/uploads")
    upload_dir.mkdir(exist_ok=True)

    # Save uploaded file
    geojson_path = upload_dir / f"{sample_id}_routes.geojson"
    try:
        content = await geojson_file.read()
        with open(geojson_path, "wb") as f:
            f.write(content)
        logger.info(f"Saved GeoJSON file to {geojson_path}")
    except Exception as e:
        logger.error(f"Failed to save GeoJSON file: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to save file: {str(e)}")

    # Create job parameters
    job_params = {
        "sample_id": sample_id,
        "geojson_path": str(geojson_path),
        "provider": provider,
        "competitor": competitor,
        "product": product,
        "mapdate": mapdate,
        "country": country,
        "endpoint": endpoint,
        "competitor_endpoint": competitor_endpoint,
        "ignore_previous_inspections": ignore_previous_inspections,
        "avoid_duplicates": avoid_duplicates
    }

    # Enqueue job
    try:
        job = inspection_queue.enqueue(
            run_inspection_pipeline,
            kwargs=job_params,
            job_timeout='2h',  # 2 hour timeout
            result_ttl=86400,  # Keep result for 24 hours
            failure_ttl=86400  # Keep failed job info for 24 hours
        )
        logger.info(f"Enqueued job {job.id} for sample {sample_id}")

        return JobSubmitResponse(
            job_id=job.id,
            status="queued",
            message="Inspection job submitted successfully",
            sample_id=sample_id,
            geojson_path=str(geojson_path)
        )
    except Exception as e:
        logger.error(f"Failed to enqueue job: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to enqueue job: {str(e)}")


@app.get("/api/v1/inspection/status/{job_id}", response_model=JobStatusResponse)
async def get_job_status(job_id: str):
    """
    Get the status of an inspection job.

    Status can be: queued, started, finished, failed, canceled
    """
    if not redis_conn:
        raise HTTPException(status_code=503, detail="Redis not available")

    try:
        job = Job.fetch(job_id, connection=redis_conn)

        response = JobStatusResponse(
            job_id=job.id,
            status=job.get_status(),
            created_at=job.created_at.isoformat() if job.created_at else None,
            started_at=job.started_at.isoformat() if job.started_at else None,
            ended_at=job.ended_at.isoformat() if job.ended_at else None
        )

        # Add result if finished
        if job.is_finished:
            response.result = job.result

        # Add error if failed
        if job.is_failed:
            response.error = str(job.exc_info) if job.exc_info else "Unknown error"

        # Add progress if available
        if hasattr(job, 'meta') and 'progress' in job.meta:
            response.progress = job.meta['progress']

        return response

    except Exception as e:
        logger.error(f"Failed to fetch job {job_id}: {e}")
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")


@app.delete("/api/v1/inspection/cancel/{job_id}")
async def cancel_job(job_id: str):
    """
    Cancel a queued or running job.
    """
    if not redis_conn:
        raise HTTPException(status_code=503, detail="Redis not available")

    try:
        job = Job.fetch(job_id, connection=redis_conn)

        if job.is_finished:
            return {"message": "Job already finished", "job_id": job_id}

        if job.is_failed:
            return {"message": "Job already failed", "job_id": job_id}

        # Cancel the job
        job.cancel()
        logger.info(f"Canceled job {job_id}")

        return {"message": "Job canceled successfully", "job_id": job_id}

    except Exception as e:
        logger.error(f"Failed to cancel job {job_id}: {e}")
        raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")


@app.get("/api/v1/inspection/queue/stats")
async def get_queue_stats():
    """
    Get statistics about the inspection queue.
    """
    if not redis_conn or not inspection_queue:
        raise HTTPException(status_code=503, detail="Redis queue not available")

    try:
        return {
            "queue_name": inspection_queue.name,
            "queued_jobs": len(inspection_queue),
            "started_jobs": inspection_queue.started_job_registry.count,
            "finished_jobs": inspection_queue.finished_job_registry.count,
            "failed_jobs": inspection_queue.failed_job_registry.count,
            "scheduled_jobs": inspection_queue.scheduled_job_registry.count,
            "workers": len(inspection_queue.workers)
        }
    except Exception as e:
        logger.error(f"Failed to get queue stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

