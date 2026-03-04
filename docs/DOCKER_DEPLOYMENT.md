# Docker Deployment Guide - TbT Inspection Pipeline

## Overview

The TbT Inspection Pipeline is now fully containerized with:
- **FastAPI** - REST API for submitting inspection jobs
- **Redis Queue (RQ)** - Asynchronous job processing
- **PostgreSQL** - Data storage
- **RQ Workers** - Parallel job execution
- **RQ Dashboard** - Job monitoring UI

## Architecture

```
┌─────────────────┐
│   Client/User   │
└────────┬────────┘
         │ HTTP (GeoJSON)
         ▼
┌─────────────────┐
│   FastAPI API   │ (Port 8000)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Redis Queue   │ (Port 6379)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   RQ Workers    │ (2 replicas)
│  - Run Pipeline │
│  - Export Data  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   PostgreSQL    │ (Port 5432)
└─────────────────┘
```

## Quick Start

### 1. Prerequisites

- Docker 20.10+
- Docker Compose 2.0+
- 8GB RAM minimum
- 20GB disk space

### 2. Configuration

```bash
# Copy environment template
cp .env.example .env

# Edit database credentials
nano .env
```

Required variables in `.env`:
```dotenv
DB_HOST=postgres
DB_PORT=5432
DB_NAME=mapex_tbt
DB_USER=tbt_user
DB_PASSWORD=your_secure_password
DB_SCHEMA=public
```

### 3. Start Services

```bash
# Make scripts executable
chmod +x docker-start.sh docker-stop.sh

# Start all services
./docker-start.sh
```

Or manually:
```bash
docker-compose up -d
```

### 4. Verify Services

```bash
# Check service status
docker-compose ps

# Check API health
curl http://localhost:8000/health

# View logs
docker-compose logs -f tbt-api
```

## Using the API

### Submit Inspection Job

Using curl:
```bash
curl -X POST http://localhost:8000/api/v1/inspection/submit \
  -F "geojson_file=@my_routes.geojson" \
  -F "provider=Orbis" \
  -F "competitor=Genesis" \
  -F "mapdate=2026-03-03"
```

Using Python:
```python
import requests

# Submit job
with open('my_routes.geojson', 'rb') as f:
    files = {'geojson_file': f}
    data = {
        'provider': 'Orbis',
        'competitor': 'Genesis',
        'mapdate': '2026-03-03'
    }
    response = requests.post(
        'http://localhost:8000/api/v1/inspection/submit',
        files=files,
        data=data
    )
    
result = response.json()
job_id = result['job_id']
print(f"Job ID: {job_id}")
```

### Check Job Status

```bash
# Get job status
curl http://localhost:8000/api/v1/inspection/status/{job_id}
```

### Cancel Job

```bash
curl -X DELETE http://localhost:8000/api/v1/inspection/cancel/{job_id}
```

### Queue Statistics

```bash
curl http://localhost:8000/api/v1/inspection/queue/stats
```

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | API information |
| `/health` | GET | Health check |
| `/docs` | GET | Interactive API documentation |
| `/api/v1/inspection/submit` | POST | Submit inspection job |
| `/api/v1/inspection/status/{job_id}` | GET | Get job status |
| `/api/v1/inspection/cancel/{job_id}` | DELETE | Cancel job |
| `/api/v1/inspection/queue/stats` | GET | Queue statistics |

## Monitoring

### RQ Dashboard

Access the RQ Dashboard at: http://localhost:9181

Features:
- View queued, running, finished, and failed jobs
- See worker status
- Monitor job execution times
- Retry failed jobs

### Logs

```bash
# API logs
docker-compose logs -f tbt-api

# Worker logs
docker-compose logs -f tbt-worker

# All logs
docker-compose logs -f

# PostgreSQL logs
docker-compose logs -f postgres

# Redis logs
docker-compose logs -f redis
```

### Database Access

```bash
# Connect to PostgreSQL
docker-compose exec postgres psql -U tbt_user -d mapex_tbt

# Check tables
\dt

# View recent inspections
SELECT * FROM inspection_metadata ORDER BY created_at DESC LIMIT 5;
```

## Scaling

### Add More Workers

Edit `docker-compose.yml`:
```yaml
tbt-worker:
  deploy:
    replicas: 4  # Increase from 2 to 4
```

Then restart:
```bash
docker-compose up -d --scale tbt-worker=4
```

### Adjust Resources

Edit `docker-compose.yml` to add resource limits:
```yaml
tbt-worker:
  deploy:
    resources:
      limits:
        cpus: '2'
        memory: 4G
      reservations:
        cpus: '1'
        memory: 2G
```

## Troubleshooting

### API Not Responding

```bash
# Check if container is running
docker-compose ps tbt-api

# Check logs
docker-compose logs tbt-api

# Restart API
docker-compose restart tbt-api
```

### Workers Not Processing Jobs

```bash
# Check worker status
docker-compose logs tbt-worker

# Check Redis connection
docker-compose exec redis redis-cli ping

# Restart workers
docker-compose restart tbt-worker
```

### Database Connection Issues

```bash
# Check PostgreSQL status
docker-compose ps postgres

# Test connection
docker-compose exec postgres pg_isready

# View PostgreSQL logs
docker-compose logs postgres
```

### Out of Memory

```bash
# Check resource usage
docker stats

# Reduce number of workers
docker-compose up -d --scale tbt-worker=1
```

### Disk Space Issues

```bash
# Check disk usage
docker system df

# Clean up old containers/images
docker system prune -a

# Remove old volumes (⚠️ data loss!)
docker volume prune
```

## Maintenance

### Backup Database

```bash
# Create backup
docker-compose exec postgres pg_dump -U tbt_user mapex_tbt > backup_$(date +%Y%m%d).sql

# Restore from backup
docker-compose exec -T postgres psql -U tbt_user mapex_tbt < backup_20260303.sql
```

### Update Application

```bash
# Pull latest code
git pull

# Rebuild images
docker-compose build

# Restart services
docker-compose up -d
```

### Clean Old Data

```bash
# Connect to database
docker-compose exec postgres psql -U tbt_user -d mapex_tbt

# Delete old inspections (older than 90 days)
DELETE FROM inspection_routes WHERE created_at < NOW() - INTERVAL '90 days';
DELETE FROM inspection_critical_sections WHERE created_at < NOW() - INTERVAL '90 days';
DELETE FROM critical_sections_with_mcp_feedback WHERE created_at < NOW() - INTERVAL '90 days';
DELETE FROM error_logs WHERE created_at < NOW() - INTERVAL '90 days';
DELETE FROM inspection_metadata WHERE created_at < NOW() - INTERVAL '90 days';
```

## Production Deployment

### Security

1. **Change default passwords** in `.env`
2. **Use HTTPS** with reverse proxy (nginx, traefik)
3. **Enable authentication** on API endpoints
4. **Restrict network access** using firewall rules
5. **Use secrets management** (Docker secrets, Vault)

### Performance

1. **Use production WSGI server** (already using uvicorn)
2. **Add caching** (Redis cache)
3. **Enable monitoring** (Prometheus, Grafana)
4. **Configure logging** (ELK stack, CloudWatch)
5. **Use persistent volumes** for data

### High Availability

```yaml
# Example docker-compose.override.yml for HA
services:
  tbt-api:
    deploy:
      replicas: 3
      restart_policy:
        condition: on-failure
  
  tbt-worker:
    deploy:
      replicas: 5
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DB_HOST` | postgres | Database hostname |
| `DB_PORT` | 5432 | Database port |
| `DB_NAME` | mapex_tbt | Database name |
| `DB_USER` | tbt_user | Database username |
| `DB_PASSWORD` | - | Database password (required) |
| `DB_SCHEMA` | public | Database schema |
| `REDIS_HOST` | redis | Redis hostname |
| `REDIS_PORT` | 6379 | Redis port |

## Testing

### Test API

```bash
# Run test script
python3 scripts/test_api.py my_routes.geojson
```

### Test Database Connection

```bash
# Run inside container
docker-compose exec tbt-api python scripts/test_database_connection.py
```

## Stopping Services

```bash
# Stop services (keep data)
./docker-stop.sh

# Or manually
docker-compose down

# Stop and remove volumes (⚠️ data loss!)
docker-compose down -v
```

## Support

For issues or questions:
1. Check logs: `docker-compose logs`
2. Verify configuration in `.env`
3. Check service health: `docker-compose ps`
4. Review API docs: http://localhost:8000/docs
5. Monitor RQ Dashboard: http://localhost:9181

