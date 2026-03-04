
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
- [Pipeline Steps Description](#pipeline-steps-description)
- [Scripts](#scripts)
- [Output data](#output-data)
- [Development](#development)
- [FAQ](docs/FAQ.md)

---

## Overview

The project computes the **TbT (Turn-by-Turn) metric** by comparing routing quality between a provider under test and a reference competitor.

### What does it do?

The pipeline performs automated quality assessment of routing providers by:

1. **Pre-inspection** (Steps 1-3)
   - Cleans stale data from previous runs
   - Creates empty Parquet schemas for outputs
   - Loads route definitions from PostgreSQL database (or GeoJSON file as fallback)
   - Generates sampling files with route metadata

2. **Core inspection** (Steps 4-8)
   - Computes routes using **provider** API (e.g., Orbis, HERE, Google)
   - Computes routes using **competitor** API (e.g., Genesis, TomTom)
   - Analyzes route geometry differences using **RAC** (Route Adherence Classification)
   - Applies **ML-based FCD** (Floating Car Data) model for error classification
   - Identifies critical sections where routes differ significantly

3. **Post-inspection** (Steps 9-14)
   - Merges results with historical data
   - Performs sanity checks on data quality
   - Exports to multiple formats:
     - **CSV** files for human review (`output/` directory)
     - **Parquet** files for data analysis (`data/tbt/inspection/`)
     - **PostgreSQL** database for integration (error logs → `leads` table in JSONB format)

### Key Features

✅ **Database-driven** - Route data from PostgreSQL, no file uploads needed  
✅ **API-agnostic** - Supports multiple routing providers (Orbis, HERE, Google, TomTom, etc.)  
✅ **ML-powered** - Automated error classification using FCD model  
✅ **Scalable** - Async processing via Redis Queue, multiple workers  
✅ **Historical tracking** - Reuses previously computed routes, maintains run history  
✅ **Multi-format export** - CSV, Parquet, PostgreSQL with JSONB leads  

### Technology Stack

The pipeline is implemented with:
- **[Kedro 0.18.4](https://kedro.readthedocs.io/)** - Pipeline orchestration framework
- **[PySpark 3.3.2](https://spark.apache.org/)** - Distributed data processing
- **[FastAPI](https://fastapi.tiangolo.com/)** - REST API for job submission
- **[Redis Queue](https://python-rq.org/)** - Asynchronous job processing
- **[PostgreSQL](https://www.postgresql.org/)** - Data storage and retrieval

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

# 3. Submit inspection job via API with pipeline_id from database
curl -X POST http://localhost:8000/api/v1/inspection/submit \
  -F "pipeline_id=b294bb07-b9d6-4e6f-8100-b909fe6227df" \
  -F "provider=Orbis" \
  -F "competitor=Genesis"

# 4. Check job status
curl http://localhost:8000/api/v1/inspection/status/{job_id}
```

**Note:** The system now reads route data from the PostgreSQL database table `tbt.pipeline_routes` based on the provided `pipeline_id`. See [docs/DATABASE_INPUT.md](docs/DATABASE_INPUT.md) for details.

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
tbt_initialize_sampling_data        (sampling from database/GeoJSON)
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
          ├─► tbt_export_to_database_node (PostgreSQL export + leads)
          │
          ▼
tbt_sanity_check_result             (final validation)
```

---

## Pipeline Steps Description

The TbT inspection pipeline consists of three main phases with multiple steps:

### Phase 1: Pre-Inspection (Preparation)

#### Step 1: Clean Data Directories
**Node:** `tbt_clean_data_directories`  
**Purpose:** Removes stale data from previous runs  
**Details:**
- Cleans `data/tbt/inspection/` directory
- Cleans `data/tbt/sampling/` directory
- Ensures fresh start for new inspection run
- Can be skipped by setting `skip_cleanup: True` in config

#### Step 2: Initialize Inspection Data
**Node:** `tbt_initialize_inspection_data`  
**Purpose:** Creates empty Parquet files with correct schemas  
**Details:**
- Generates empty `inspection_routes.parquet`
- Generates empty `inspection_critical_sections.parquet`
- Generates empty `critical_sections_with_mcp_feedback.parquet`
- Generates empty `inspection_metadata.parquet`
- Ensures all output files exist even if pipeline fails mid-execution

#### Step 3: Initialize Sampling Data
**Node:** `tbt_initialize_sampling_data`  
**Purpose:** Prepares route data for inspection  
**Details:**
- **Database mode** (if `pipeline_id` provided): Fetches routes from PostgreSQL table `tbt.pipeline_routes`
- **Legacy mode** (if no `pipeline_id`): Reads routes from `li_input/geojson/Routes2check.geojson`
- Generates `sampling_samples.parquet` (one row per route)
- Generates `sampling_metadata.parquet` (one row per sample)
- All routes are tagged with `sample_id` for tracking

### Phase 2: Core Inspection (Route Computation & Analysis)

#### Step 4: Get Provider Routes
**Node:** `tbt_provider_routes_node`  
**Purpose:** Computes routes using the provider under test  
**Details:**
- Calls provider routing API (Orbis, HERE, Google, etc.)
- Processes all routes from sampling data filtered by `sample_id`
- Extracts route geometry, travel time, distance
- Handles API errors and timeouts
- Records API call statistics

#### Step 5: Reuse Static Routes
**Node:** `tbt_reuse_static_routes_node`  
**Purpose:** Reuses previously computed routes to avoid redundant API calls  
**Details:**
- Checks if routes were already computed in previous runs
- Loads cached routes from historical data
- Merges with newly computed routes
- Reduces API costs and execution time

#### Step 6: Get Competitor Routes
**Node:** `tbt_competitor_routes_node`  
**Purpose:** Computes routes using the reference competitor  
**Details:**
- Calls competitor routing API (Genesis, TomTom, etc.)
- Uses same origin/destination pairs as provider
- Extracts route geometry, travel time, distance
- Provides baseline for comparison

#### Step 7: Compute RAC State
**Node:** `tbt_get_rac_state`  
**Purpose:** Computes Route Adherence Classification (RAC) state  
**Details:**
- Compares provider route vs competitor route geometries
- Classifies each route pair:
  - `match` - routes are similar
  - `no_match` - routes differ significantly
  - `error` - computation failed
- Uses geometric algorithms (Fréchet distance, Hausdorff distance)
- Identifies critical sections where routes diverge

#### Step 8: Compute FCD State
**Node:** `tbt_get_fcd_state`  
**Purpose:** Computes Floating Car Data (FCD) state using ML model  
**Details:**
- Analyzes critical sections identified by RAC
- Applies ML model to classify errors:
  - `true_error` - confirmed routing error
  - `potential_error` - requires manual review
  - `no_error` - acceptable difference
- Computes metrics: PRA, PRB, PRAB, LIFT, TOT
- Enriches critical sections with FCD classification

### Phase 3: Post-Inspection (Export & Validation)

#### Step 9: Merge Inspection Data
**Node:** `tbt_merge_inspection_data`  
**Purpose:** Aggregates all results into final output tables  
**Details:**
- Combines new routes with historical data
- Combines new critical sections with historical data
- Generates error logs (routes requiring manual review)
- Creates inspection metadata (run statistics, timing, API calls)
- Assigns unique `run_id` to this inspection run

#### Step 10: Sanity Check
**Node:** `tbt_sanity_check_node`  
**Purpose:** Validates data quality before export  
**Details:**
- Checks if number of routes matches expected count
- Validates critical sections are properly classified
- Ensures metadata contains required fields
- Flags suspicious results (e.g., 100% error rate)
- Passes data through if quality checks pass

#### Step 11: Export to CSV
**Node:** `tbt_export_to_sql_node`  
**Purpose:** Exports results to CSV files for easy inspection  
**Details:**
- Writes to `output/` directory
- Creates: `inspection_routes.csv`, `inspection_critical_sections.csv`, 
  `critical_sections_with_mcp_feedback.csv`, `error_logs.csv`, `inspection_metadata.csv`
- Human-readable format for quick review

#### Step 12: Export to Spark
**Node:** `tbt_export_to_spark_node`  
**Purpose:** Exports results to Spark Parquet format  
**Details:**
- Writes to `data/tbt/inspection/` directory
- Parquet format for efficient storage and processing
- Preserves data types and schema
- Optimized for large-scale data analysis

#### Step 13: Export to Database
**Node:** `tbt_export_to_database_node`  
**Purpose:** Exports results to PostgreSQL database  
**Details:**
- Connects to PostgreSQL using credentials from `.env`
- Exports all inspection tables to configured schema
- **Special handling for error_logs:**
  - Converts to `leads` table format
  - Stores as JSONB in `lead_data` column
  - Tags with `pipeline_id` and `source='tbt'`
- Enables integration with other systems
- See [docs/LEADS_EXPORT.md](docs/LEADS_EXPORT.md) for details

#### Step 14: Final Sanity Check
**Node:** `tbt_sanity_check_result`  
**Purpose:** Final validation after all exports  
**Details:**
- Verifies all exports completed successfully
- Checks export flags from all export nodes
- Raises error if critical issues detected
- Marks inspection as complete if all checks pass

---

## Data Flow Summary

```
Input: pipeline_id (or GeoJSON file)
   ↓
Database/File: Route definitions
   ↓
Provider API: Compute provider routes
   ↓
Competitor API: Compute competitor routes
   ↓
RAC Analysis: Compare route geometries
   ↓
FCD Analysis: ML-based error classification
   ↓
Merge: Combine with historical data
   ↓
Validate: Sanity checks
   ↓
Export: CSV + Parquet + PostgreSQL (leads table)
   ↓
Output: Inspection results ready for review
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

After a successful run, results are available in multiple forms:

**Parquet** (in `data/tbt/inspection/`):

| File | Description |
|---|---|
| `inspection_routes` | Provider vs competitor routes with RAC state |
| `inspection_critical_sections` | Critical sections with FCD state |
| `critical_sections_with_mcp_feedback` | MCP-annotated critical sections |
| `inspection_metadata` | Run metadata (timing, API calls, sanity status) |
| `error_logs` | Per-route error details |

**CSV** (in `output/`): same files exported as flat CSV for easy inspection.

**Database** (PostgreSQL):
- All inspection tables are exported to the database schema (configured in `.env`)
- **Error logs** are additionally exported to `leads` table in JSONB format:
  - `pipeline_id`: Identifies the inspection run
  - `source`: Always `'tbt'` for TbT inspections
  - `lead_data`: JSONB with all error log fields
  - See [docs/LEADS_EXPORT.md](docs/LEADS_EXPORT.md) for details and query examples

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
