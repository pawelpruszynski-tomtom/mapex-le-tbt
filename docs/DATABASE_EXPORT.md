# Database Export Feature

## Overview

The post-inspection pipeline includes a database export step that saves all inspection results to a PostgreSQL database. This feature runs in parallel with CSV and Spark exports.

## Implementation

### Architecture

The database export is implemented as a separate node in the `post_inspection` pipeline:

```
sanity_check
     │
     ├─► export_to_csv (CSV files)
     │
     ├─► export_to_spark (Parquet files)
     │
     ├─► export_to_database (PostgreSQL)
     │
     ▼
raise_sanity_error (final validation)
```

### Files Modified

1. **src/tbt/pipelines/inspection/nodes/export.py**
   - Added `export_to_database()` function
   - Uses SQLAlchemy for database connections
   - Reads credentials from environment variables via python-dotenv

2. **src/tbt/pipelines/inspection/pipelines/post_inspection.py**
   - Added `tbt_export_to_database_node` to the pipeline
   - Updated `raise_sanity_error` node to include database export flag

3. **src/tbt/pipelines/inspection/nodes/sanity.py**
   - Updated `raise_sanity_error()` to accept `export_to_database_ok` parameter

4. **conf/local/catalog.yml** and **conf/dev/catalog.yml**
   - Added MemoryDataSet entries for database output datasets

5. **src/requirements.txt**
   - Added `python-dotenv~=1.0.0` for environment variable management
   - Added `psycopg2-binary~=2.9.9` for PostgreSQL connectivity

## Configuration

### Environment Variables

Create a `.env` file in the project root with the following variables:

```dotenv
DB_HOST=localhost          # Database hostname or IP
DB_PORT=5432              # Database port (default: 5432)
DB_NAME=mapex_tbt         # Database name
DB_USER=your_username     # Database username
DB_PASSWORD=your_password # Database password
DB_SCHEMA=public          # Database schema (optional, defaults to 'public')
```

### Security

- The `.env` file is automatically ignored by git (already in `.gitignore`)
- Never commit credentials to version control
- Use `.env.example` as a template for new environments

## Database Schema

The export creates/updates the following tables:

### inspection_routes
Routes with provider and competitor data and RAC state.

### inspection_critical_sections
Critical sections with FCD state and metrics (pra, prb, prab, lift, tot).

### critical_sections_with_mcp_feedback
Critical sections with MCP feedback and error classification.

### error_logs
Per-route error details for failed routes.

### inspection_metadata
Run metadata including timing, API calls, and sanity check results.

## Usage

### Running the Pipeline

The database export runs automatically as part of the full inspection pipeline:

```bash
kedro run --pipeline=tbt_inspection
```

Or run only the post-inspection step:

```bash
kedro run --pipeline=tbt_inspection_post
```

### Data Handling

- **Append mode**: By default, data is appended to existing tables
- **Chunked writes**: Data is written in chunks of 1000 rows for performance
- **Multi-method**: Uses SQLAlchemy's multi-row insert for efficiency

### Error Handling

If database export fails:
1. The error is logged with full details
2. The exception is re-raised to fail the pipeline
3. CSV and Spark exports still succeed (they run in parallel)

## Troubleshooting

### Connection Issues

**Problem**: "Missing required database credentials"
**Solution**: Ensure `.env` file exists and contains all required variables (DB_HOST, DB_NAME, DB_USER, DB_PASSWORD)

**Problem**: "could not connect to server"
**Solution**: 
- Check if PostgreSQL is running
- Verify host and port are correct
- Check firewall rules

### Performance

**Problem**: Slow export for large datasets
**Solution**: 
- Increase `chunksize` parameter in `export_to_database()` function
- Consider creating indexes on frequently queried columns
- Use connection pooling for multiple runs

### Schema Changes

**Problem**: "Column X does not exist"
**Solution**: 
- Drop and recreate tables, or
- Use `if_exists='replace'` instead of `'append'` in `to_sql()` calls

## Development

### Testing

To test database export without running the full pipeline:

```python
from tbt.pipelines.inspection.nodes import export_to_database
import pandas as pd
import os

# Set up test environment
os.environ['DB_HOST'] = 'localhost'
os.environ['DB_NAME'] = 'test_db'
os.environ['DB_USER'] = 'test_user'
os.environ['DB_PASSWORD'] = 'test_pass'

# Create test DataFrames
test_routes = pd.DataFrame({'route_id': ['1', '2'], 'country': ['DE', 'PL']})
# ... create other DataFrames ...

# Run export
export_to_database(test_routes, test_sections, test_feedback, test_errors, test_metadata)
```

### Extending

To add new tables or modify the export:

1. Update `export_to_database()` in `nodes/export.py`
2. Add corresponding MemoryDataSet entries in catalog files
3. Update pipeline nodes to include new outputs
4. Document schema changes

## Alternative Database Systems

While the current implementation uses PostgreSQL, it can be adapted for other databases:

### MySQL
Change connection string format:
```python
connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
```
Add to requirements.txt: `pymysql~=1.0.2`

### SQL Server
Change connection string format:
```python
connection_string = f"mssql+pyodbc://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server"
```
Add to requirements.txt: `pyodbc~=4.0.39`

### SQLite
Change connection string format:
```python
connection_string = f"sqlite:///{db_name}"
```
No additional requirements needed (included in Python standard library).

