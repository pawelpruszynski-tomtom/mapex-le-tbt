# Konteneryzacja Pipeline TbT - Podsumowanie

## 🎯 Cel
Skonteneryzowano pipeline TbT Inspection z obsługą Redis Queue, umożliwiając:
- Wysyłanie zadań przez API z plikami GeoJSON
- Asynchroniczne przetwarzanie w tle
- Skalowanie workerów
- Monitoring zadań

## 📦 Co zostało dodane

### 1. Konteneryzacja

#### Dockerfile
- Multi-stage build dla optymalizacji rozmiaru
- Java 11 dla PySpark
- Python 3.9 + wszystkie zależności
- FastAPI + Redis Queue + PostgreSQL client
- Struktura katalogów /app

#### docker-compose.yml
5 serwisów:
- **tbt-api** - FastAPI REST API (port 8000)
- **tbt-worker** - RQ workers (2 repliki)
- **redis** - Queue backend (port 6379)
- **postgres** - Baza danych (port 5432)
- **rq-dashboard** - UI monitoring (port 9181)

### 2. FastAPI REST API

#### Endpointy

| Endpoint | Metoda | Opis |
|----------|--------|------|
| `/` | GET | Informacje o API |
| `/health` | GET | Health check |
| `/docs` | GET | Dokumentacja Swagger |
| `/api/v1/inspection/submit` | POST | Wysłanie GeoJSON |
| `/api/v1/inspection/status/{job_id}` | GET | Status zadania |
| `/api/v1/inspection/cancel/{job_id}` | DELETE | Anulowanie zadania |
| `/api/v1/inspection/queue/stats` | GET | Statystyki kolejki |

#### Funkcjonalności
- ✅ Upload plików GeoJSON
- ✅ Walidacja plików
- ✅ Automatyczne generowanie sample_id
- ✅ Konfiguracja parametrów (provider, competitor, mapdate, etc.)
- ✅ Enqueue do Redis Queue
- ✅ Tracking statusu (queued, started, finished, failed)
- ✅ Obsługa błędów

### 3. Redis Queue Worker

#### worker.py
- Funkcja `run_inspection_pipeline()` wykonywana przez RQ
- Bootstrap Kedro project
- Kopiowanie GeoJSON do właściwej lokalizacji
- Uruchomienie pipeline `tbt_inspection`
- Zbieranie rezultatów (CSV, Parquet, Database)
- Zwracanie szczegółowego wyniku

#### Funkcjonalności
- ✅ Timeout 2h na zadanie
- ✅ Result TTL 24h
- ✅ Failure TTL 24h
- ✅ Szczegółowe logowanie
- ✅ Obsługa wyjątków

### 4. Python Client

#### client.py
Klasa `TbtInspectionClient`:
- `health_check()` - Sprawdzenie statusu API
- `submit_inspection()` - Wysłanie zadania
- `get_job_status()` - Pobranie statusu
- `cancel_job()` - Anulowanie zadania
- `get_queue_stats()` - Statystyki kolejki
- `wait_for_completion()` - Czekanie na zakończenie
- `submit_and_wait()` - Submit + wait (convenience method)

### 5. Skrypty pomocnicze

- **docker-start.sh** - Start wszystkich serwisów
- **docker-stop.sh** - Stop serwisów
- **scripts/test_api.py** - Test API z linii poleceń
- **.dockerignore** - Ignorowanie niepotrzebnych plików

### 6. Przykłady użycia

- **examples/api_client_usage.py** - Użycie Python client
- **examples/direct_rq_usage.py** - Bezpośrednie użycie RQ

### 7. Dokumentacja

- **docs/DOCKER_DEPLOYMENT.md** - Kompletna dokumentacja deployu
  - Quick start
  - Architektura
  - API reference
  - Monitoring
  - Scaling
  - Troubleshooting
  - Production deployment

## 🚀 Użycie

### Metoda 1: Przez REST API (curl)

```bash
# Start serwisów
./docker-start.sh

# Wyślij zadanie
curl -X POST http://localhost:8000/api/v1/inspection/submit \
  -F "geojson_file=@my_routes.geojson" \
  -F "provider=Orbis" \
  -F "competitor=Genesis"

# Sprawdź status
curl http://localhost:8000/api/v1/inspection/status/{job_id}
```

### Metoda 2: Przez Python Client

```python
from tbt_api.client import TbtInspectionClient

client = TbtInspectionClient()

# Submit i czekaj na wynik
result = client.submit_and_wait(
    geojson_file="routes.geojson",
    provider="Orbis",
    competitor="Genesis"
)

print(f"Status: {result['status']}")
print(f"Results: {result['result']}")
```

### Metoda 3: Przez RQ bezpośrednio

```python
import redis
from rq import Queue
from tbt_api.worker import run_inspection_pipeline

redis_conn = redis.Redis()
queue = Queue('tbt-inspection', connection=redis_conn)

job = queue.enqueue(
    run_inspection_pipeline,
    kwargs={
        "sample_id": "test-001",
        "geojson_path": "/app/uploads/routes.geojson",
        "provider": "Orbis",
        "competitor": "Genesis"
    }
)

print(f"Job ID: {job.id}")
```

### Metoda 4: Test script

```bash
python scripts/test_api.py my_routes.geojson
```

## 📊 Monitoring

### RQ Dashboard
```
http://localhost:9181
```
Funkcje:
- Lista zadań (queued, running, finished, failed)
- Status workerów
- Czasy wykonania
- Retry failed jobs

### API Swagger Docs
```
http://localhost:8000/docs
```
Interaktywna dokumentacja API z możliwością testowania.

### Logs
```bash
# Wszystkie serwisy
docker-compose logs -f

# Tylko API
docker-compose logs -f tbt-api

# Tylko workers
docker-compose logs -f tbt-worker
```

## 🔧 Konfiguracja

### .env (wymagane)
```dotenv
DB_HOST=postgres
DB_PORT=5432
DB_NAME=mapex_tbt
DB_USER=tbt_user
DB_PASSWORD=secure_password
DB_SCHEMA=public
REDIS_HOST=redis
REDIS_PORT=6379
```

### Skalowanie workerów
```bash
# Zwiększ liczbę workerów
docker-compose up -d --scale tbt-worker=4

# Lub edytuj docker-compose.yml
tbt-worker:
  deploy:
    replicas: 4
```

## 📁 Struktura plików

```
mapex-le-tbt/
├── Dockerfile                      # Definicja obrazu Docker
├── docker-compose.yml              # Orkiestracja serwisów
├── .dockerignore                   # Pliki ignorowane przez Docker
├── docker-start.sh                 # Skrypt start
├── docker-stop.sh                  # Skrypt stop
├── src/
│   └── tbt_api/
│       ├── __init__.py
│       ├── main.py                 # FastAPI app
│       ├── worker.py               # RQ worker function
│       └── client.py               # Python client
├── scripts/
│   └── test_api.py                 # Test script
├── examples/
│   ├── api_client_usage.py         # Przykład użycia client
│   └── direct_rq_usage.py          # Przykład użycia RQ
└── docs/
    └── DOCKER_DEPLOYMENT.md        # Dokumentacja
```

## 🎯 Workflow

```
1. Client/User
      │
      ▼ POST /api/v1/inspection/submit (GeoJSON)
2. FastAPI API
      │
      ├─ Walidacja pliku
      ├─ Generowanie sample_id
      ├─ Zapis GeoJSON do /app/uploads
      │
      ▼ Enqueue job
3. Redis Queue
      │
      ▼ Job picked by worker
4. RQ Worker
      │
      ├─ Bootstrap Kedro
      ├─ Kopiowanie GeoJSON
      ├─ Uruchomienie pipeline
      │   ├─ Pre-inspection
      │   ├─ Core-inspection
      │   └─ Post-inspection
      │       ├─ Export CSV
      │       ├─ Export Spark
      │       └─ Export Database
      │
      ▼ Return result
5. Result stored in Redis (24h)
      │
      ▼ GET /api/v1/inspection/status/{job_id}
6. Client receives result
```

## ✨ Kluczowe funkcjonalności

### Asynchroniczność
- Zadania przetwarzane w tle
- API nie blokuje na czas wykonania
- Możliwość wysłania wielu zadań równocześnie

### Skalowalność
- Łatwe dodawanie workerów
- Równoległe przetwarzanie zadań
- Resource limits w docker-compose

### Monitoring
- RQ Dashboard dla wizualizacji kolejki
- Health check endpoint
- Szczegółowe logi

### Bezpieczeństwo
- Credentials w .env (nie w repo)
- Izolacja serwisów w Docker network
- Możliwość dodania auth do API

### Trwałość danych
- PostgreSQL z persistent volume
- Redis z persistent volume
- Rezultaty trzymane 24h

## 🔍 Troubleshooting

### API nie odpowiada
```bash
docker-compose logs tbt-api
docker-compose restart tbt-api
```

### Workery nie przetwarzają
```bash
docker-compose logs tbt-worker
docker-compose exec redis redis-cli ping
docker-compose restart tbt-worker
```

### Brak połączenia z bazą
```bash
docker-compose logs postgres
docker-compose exec postgres pg_isready
```

### Brak miejsca na dysku
```bash
docker system df
docker system prune -a
```

## 🚀 Production Ready

### Security checklist
- ✅ Zmień domyślne hasła w .env
- ✅ Użyj HTTPS (nginx reverse proxy)
- ✅ Włącz auth na API endpoints
- ✅ Ogranicz dostęp sieciowy (firewall)
- ✅ Użyj secrets management (Docker secrets, Vault)

### Performance checklist
- ✅ Dostosuj liczbę workerów do CPU
- ✅ Skonfiguruj resource limits
- ✅ Włącz monitoring (Prometheus, Grafana)
- ✅ Skonfiguruj logging (ELK, CloudWatch)
- ✅ Używaj persistent volumes

### High Availability
- ✅ Multiple API replicas (load balancer)
- ✅ Multiple workers
- ✅ Redis persistence (RDB + AOF)
- ✅ PostgreSQL replication
- ✅ Health checks i restart policies

## 📚 Dokumentacja

- **README.md** - Główna dokumentacja projektu (zaktualizowana)
- **docs/DOCKER_DEPLOYMENT.md** - Kompletny deployment guide
- **docs/DATABASE_EXPORT.md** - Dokumentacja eksportu do bazy
- **docs/QUICKSTART_DATABASE.md** - Quick start dla bazy danych

## ✅ Testowanie

Wszystkie komponenty przetestowane:
- ✅ Dockerfile builds successfully
- ✅ docker-compose starts all services
- ✅ API responds to health check
- ✅ Redis connection works
- ✅ PostgreSQL connection works
- ✅ Worker can process jobs
- ✅ Python client works

## 🎉 Gotowe!

Projekt jest w pełni skonteneryzowany i gotowy do:
1. Development (lokalne testy)
2. CI/CD integration
3. Production deployment
4. Kubernetes migration (w przyszłości)

### Następne kroki:
1. `cp .env.example .env` i edytuj credentials
2. `./docker-start.sh`
3. Otwórz http://localhost:8000/docs
4. Wyślij pierwszy job!

