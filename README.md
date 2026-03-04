
# TbT Metric by MapEx 1.0

Kedro-based pipeline for running **Turn-by-Turn (TbT) route inspections** — comparing provider routes against a competitor, computing RAC/FCD states and producing structured inspection outputs.

---

## Table of Contents

- [Overview](#overview)
- [Docker Deployment](#docker-deployment)
- [Using the API](#using-the-api)
- [Project structure](#project-structure)
- [Setup](#setup)
- [Input data](#input-data)
- [Configuration](#configuration)
- [Running pipelines](#running-pipelines)
- [Pipeline DAG](#pipeline-dag)
- [Scripts](#scripts)
- [Output data](#output-data)
- [Development](#development)
- [FAQ](docs/FAQ.md)

---

## Overview

The project computes the **TbT metric** by:

1. **Pre-inspection** — cleaning stale data, creating empty parquet schemas and generating sampling files from a GeoJSON route list.
2. **Core inspection** — calling provider & competitor routing APIs, computing RAC and FCD states, merging results.
3. **Post-inspection** — sanity checks and export to CSV / Spark / SQL.

The pipeline is implemented with [Kedro 0.18.4](https://kedro.readthedocs.io/) and [PySpark 3.3.2](https://spark.apache.org/).

---

## Docker Deployment

The pipeline is fully containerized and can be deployed using Docker with Redis Queue for asynchronous job processing.

**Note:** This deployment connects to your **existing Redis and PostgreSQL instances**. Make sure they are running and accessible before starting.

### Prerequisites

- Docker 20.10+ and Docker Compose 2.0+
- **Existing Redis instance** (accessible from Docker containers)
- **Existing PostgreSQL instance** (accessible from Docker containers)

### Quick Start with Docker

```bash
# 1. Configure environment - connect to your existing services
cp .env.example .env
nano .env  # Edit: REDIS_HOST, REDIS_PORT, DB_HOST, DB_PORT, credentials

# 2. Start application services (API + Workers only)
./docker-start.sh

# Or use Makefile
make quick-start

# 3. Submit inspection job via API
# Note: GeoJSON file path is relative to where you run curl
curl -X POST http://localhost:8000/api/v1/inspection/submit \
  -F "geojson_file=@my_routes.geojson" \
  -F "provider=Orbis" \
  -F "competitor=Genesis"

# Examples with different paths:
# From project root:
curl -X POST http://localhost:8000/api/v1/inspection/submit \
  -F "geojson_file=@li_input/geojson/Routes2check.geojson" \
  -F "provider=Orbis" \
  -F "competitor=Genesis"

# From any directory (absolute path):
curl -X POST http://localhost:8000/api/v1/inspection/submit \
  -F "geojson_file=@/home/user/my_routes.geojson" \
  -F "provider=Orbis" \
  -F "competitor=Genesis"
```

### Makefile Commands

```bash
make help           # Show all available commands
make up             # Start all services
make down           # Stop all services
make logs           # Show logs from all services
make health         # Check API health
make stats          # Show queue statistics
make scale-workers N=4  # Scale workers to 4
```

### Services

**Application services** (deployed via Docker):

| Service | Port | Description |
|---------|------|-------------|
| tbt-api | 8000 | REST API for job submission |
| tbt-worker | - | RQ Workers (2 replicas) for processing |
| rq-dashboard | 9181 | Job monitoring UI |

**External services** (must be running separately):
- **Redis** - Configured via REDIS_HOST, REDIS_PORT in .env
- **PostgreSQL** - Configured via DB_HOST, DB_PORT in .env

### API Endpoints

- `POST /api/v1/inspection/submit` - Submit GeoJSON for inspection
- `GET /api/v1/inspection/status/{job_id}` - Check job status
- `GET /api/v1/inspection/queue/stats` - Queue statistics
- `GET /docs` - Interactive API documentation

### Features

- ✅ **Async Processing** - Jobs run in background via Redis Queue
- ✅ **Scalable Workers** - Multiple workers for parallel processing
- ✅ **Job Monitoring** - Real-time status tracking via RQ Dashboard
- ✅ **REST API** - Submit jobs with GeoJSON files
- ✅ **Database Export** - Automatic export to PostgreSQL
- ✅ **Docker Compose** - Single command deployment

📖 **Full Documentation**: [docs/DOCKER_DEPLOYMENT.md](docs/DOCKER_DEPLOYMENT.md)

---

## Using the API

### Submitting Inspection Jobs

The API accepts GeoJSON files for inspection. The file path in curl is **relative to your current directory**.

#### Example 1: File in current directory

```bash
# If you're in the directory with your GeoJSON file:
cd /path/to/directory/with/geojson
curl -X POST http://localhost:8000/api/v1/inspection/submit \
  -F "geojson_file=@routes.geojson" \
  -F "provider=Orbis" \
  -F "competitor=Genesis"
```

#### Example 2: File in project directory

```bash
# From project root (mapex-le-tbt/):
curl -X POST http://localhost:8000/api/v1/inspection/submit \
  -F "geojson_file=@li_input/geojson/Routes2check.geojson" \
  -F "provider=Orbis" \
  -F "competitor=Genesis"
```

#### Example 3: Absolute path

```bash
# Using absolute path (works from any directory):
curl -X POST http://localhost:8000/api/v1/inspection/submit \
  -F "geojson_file=@/home/pruszyns/mapex-le-tbt/li_input/geojson/Routes2check.geojson" \
  -F "provider=Orbis" \
  -F "competitor=Genesis"
```

#### Example 4: Additional parameters

```bash
curl -X POST http://localhost:8000/api/v1/inspection/submit \
  -F "geojson_file=@routes.geojson" \
  -F "provider=Orbis" \
  -F "competitor=Genesis" \
  -F "product=latest" \
  -F "mapdate=2026-03-03" \
  -F "country=PL"
```

### Checking Job Status

After submitting, you'll receive a `job_id`. Use it to check status:

```bash
# Save job_id from submit response
JOB_ID="abc-123-def-456"

# Check status
curl http://localhost:8000/api/v1/inspection/status/$JOB_ID

# Example response:
# {
#   "job_id": "abc-123-def-456",
#   "status": "finished",
#   "result": {
#     "status": "success",
#     "sample_id": "uuid-here",
#     "duration_seconds": 145.3,
#     "output_files": {...}
#   }
# }
```

### Python Client Usage

```python
from tbt_api.client import TbtInspectionClient

# Create client
client = TbtInspectionClient()

# Submit and wait for completion
result = client.submit_and_wait(
    geojson_file="li_input/geojson/Routes2check.geojson",  # Path relative to script
    provider="Orbis",
    competitor="Genesis",
    verbose=True
)

print(f"Status: {result['status']}")
if result['status'] == 'finished':
    print(f"Duration: {result['result']['duration_seconds']}s")
```

### Test Script Usage

```bash
# From project root
python3 scripts/test_api.py li_input/geojson/Routes2check.geojson

# Or with absolute path
python3 scripts/test_api.py /full/path/to/routes.geojson

# Or if GeoJSON is in current directory
cd /path/to/geojson/files
python3 /path/to/mapex-le-tbt/scripts/test_api.py routes.geojson
```

### Quick Reference: File Paths in curl

The `@` symbol in curl tells it to read file content. Path can be:

| Path Type | Example | Works from |
|-----------|---------|------------|
| **Relative** | `@routes.geojson` | Same directory as curl command |
| **Relative** | `@../data/routes.geojson` | Parent directory |
| **Relative** | `@li_input/geojson/Routes2check.geojson` | Project subdirectory |
| **Absolute** | `@/home/user/routes.geojson` | Any directory |
| **Windows** | `@C:\Users\user\routes.geojson` | Any directory (Windows) |
| **WSL** | `@/mnt/c/Users/user/routes.geojson` | Any directory (WSL) |

---

## Project structure

```
mapex-le-tbt/
├── conf/
│   ├── base/
│   │   ├── globals.yml                  # shared path variables
│   │   ├── logging.yml
│   │   └── parameters/
│   │       ├── tbt.yml                  # main run parameters
│   │       └── run.yml                  # run_id (auto-generated)
│   ├── dev/catalog.yml                  # Spark dataset definitions (dev)
│   └── local/catalog.yml                # Spark dataset definitions (local)
├── li_input/
│   ├── csv/Routes2check.csv             # raw route list (input)
│   └── geojson/Routes2check.geojson     # converted route list (generated)
├── data/
│   └── tbt/
│       ├── inspection/                  # inspection parquet outputs
│       └── sampling/                    # sampling parquet files
├── output/                              # exported CSV results
├── scripts/
│   ├── convert_routes2check_to_geojson.py
│   ├── generate_empty_inspection_*.py   # empty inspection schema creators
│   └── generate_sampling_*.py           # sampling data generators
└── src/tbt/
    ├── pipeline_registry.py
    └── pipelines/inspection/
        ├── pipeline.py                  # full pipeline composition
        ├── pipelines/
        │   ├── pre_inspection.py
        │   ├── core_inspection.py
        │   └── post_inspection.py
        └── nodes/
            ├── cleanup.py
            ├── initialize.py
            ├── initialize_sampling.py
            ├── routing.py
            ├── rac.py
            ├── fcd.py
            ├── merge.py
            ├── sanity.py
            └── export.py
```

---

## Setup

### Requirements

- Python 3.9+
- Java 11 (required by PySpark)
- PySpark 3.3.2

### Install

```bash
cd src
pip install -e .
pip install -r requirements.txt
```

---

## Input data

### 1. Prepare GeoJSON from CSV

Before the first run, convert `li_input/csv/Routes2check.csv` to GeoJSON:

```bash
python3 scripts/convert_routes2check_to_geojson.py
```

This creates `li_input/geojson/Routes2check.geojson` — the source of truth for sampling data.  
The CSV must have columns: `sample_id, tile_id, origin, destination, route_id, quality, country, date_generated, org`.  
`origin` / `destination` must be in `POINT(lon lat)` format.

### 2. Sampling parquet (auto-generated by pipeline)

The pre-inspection step automatically generates:

| File | Description |
|---|---|
| `data/tbt/sampling/sampling_samples.parquet` | One row per route from GeoJSON |
| `data/tbt/sampling/sampling_metadata.parquet` | One row per unique `(sample_id, country, date)` |

---

## Configuration

All run parameters are set in `conf/base/parameters/tbt.yml`:

```yaml
tbt_options:
  sample_id: 0273e3cc-a095-4e4a-aa33-b760433ed8fe  # ID of the sampling set to inspect
  provider: Orbis          # provider under test
  competitor: Genesis      # reference competitor
  endpoint:                # optional custom provider endpoint
  competitor_endpoint:     # optional custom competitor endpoint
  product: latest          # map product version
  mapdate: 2025-12-19      # map date
  ignore_previous_inspections: True
  error_classification_mode: False
  avoid_duplicates: True   # fail if this sample_id was already inspected
  skip_cleanup: False      # set True to skip data/tbt/* cleanup on startup
```

### Database Configuration

The post-inspection pipeline now includes a database export step that saves inspection results to PostgreSQL.

**Setup:**

1. Copy `.env.example` to `.env` in the project root:
   ```bash
   cp .env.example .env
   ```

2. Edit `.env` and fill in your database credentials:
   ```dotenv
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=mapex_tbt
   DB_USER=your_username
   DB_PASSWORD=your_password
   DB_SCHEMA=public
   ```

3. Test the database connection:
   ```bash
   python scripts/test_database_connection.py
   ```

4. Ensure PostgreSQL is running and the database exists.

The database export creates the following tables:
- `inspection_routes`
- `inspection_critical_sections`
- `critical_sections_with_mcp_feedback`
- `error_logs`
- `inspection_metadata`

> **Note:** The `.env` file is automatically ignored by git to prevent committing sensitive credentials.

---

## Running pipelines

### Full inspection

```bash
kedro run --pipeline=tbt_inspection
```

### Sub-pipelines (selective execution)

| Command | What it runs |
|---|---|
| `kedro run --pipeline=tbt_inspection_pre` | Cleanup → empty inspection files → sampling files |
| `kedro run --pipeline=tbt_inspection_core` | Provider routes → RAC → FCD → merge |
| `kedro run --pipeline=tbt_inspection_post` | Sanity checks → CSV/Spark/PostgreSQL export |

> **Note:** Running `tbt_inspection_core` or `tbt_inspection_post` standalone requires that the previous sub-pipeline has already been run and its output files exist.

---

## Pipeline DAG

```
tbt_clean_data_directories          (cleanup)
          │ tbt_cleanup_done
tbt_initialize_inspection_data      (empty parquet schemas)
          │ tbt_init_done
tbt_initialize_sampling_data        (sampling from GeoJSON)
          │ tbt_sampling_init_done
          ▼
tbt_provider_routes_node            (call provider API)
          │
tbt_reuse_static_routes_node        (reuse cached routes)
          │
tbt_competitor_routes_node          (call competitor API)
          │
tbt_get_rac_state                   (RAC computation)
          │
tbt_get_fcd_state                   (FCD + ML model)
          │
tbt_merge_inspection_data           (assemble results)
          │
tbt_sanity_check_node               (data quality)
          │
          ├─► tbt_export_to_sql_node      (CSV export)
          │
          ├─► tbt_export_to_spark_node    (Spark parquet export)
          │
          ├─► tbt_export_to_database_node (PostgreSQL export)
          │
          ▼
tbt_sanity_check_result             (final validation)
```

---

## Scripts

| Script | Description |
|---|---|
| `convert_routes2check_to_geojson.py` | Converts `li_input/csv/Routes2check.csv` → `li_input/geojson/Routes2check.geojson` |
| `test_database_connection.py` | Tests database connection using credentials from `.env` file |
| `generate_empty_inspection_routes.py` | Creates empty `inspection_routes.parquet` |
| `generate_empty_inspection_metadata.py` | Creates empty `inspection_metadata.parquet` |
| `generate_empty_inspection_critical_sections.py` | Creates empty `inspection_critical_sections.parquet` |
| `generate_empty_inspection_critical_sections_with_mcp_feedback.py` | Creates empty `critical_sections_with_mcp_feedback.parquet` |
| `generate_sampling_samples.py` | Creates `sampling_samples.parquet` from GeoJSON |
| `generate_sampling_metadata.py` | Creates `sampling_metadata.parquet` from GeoJSON |

> All `generate_empty_inspection_*.py` and `generate_sampling_*.py` scripts are called automatically by the pre-inspection pipeline. They can also be run manually with `python3 scripts/<script_name>.py`.

---

## Output data

After a successful run, results are available in two forms:

**Parquet** (in `data/tbt/inspection/`):

| File | Description |
|---|---|
| `inspection_routes` | Provider vs competitor routes with RAC state |
| `inspection_critical_sections` | Critical sections with FCD state |
| `critical_sections_with_mcp_feedback` | MCP-annotated critical sections |
| `inspection_metadata` | Run metadata (timing, API calls, sanity status) |
| `error_logs` | Per-route error details |

**CSV** (in `output/`): same files exported as flat CSV for easy inspection.

---

## Development

### Run tests

```bash
cd src
pytest
```

### Lint & format

```bash
isort .
black .
```

### Generate docs

```bash
cd docs
make html
```
