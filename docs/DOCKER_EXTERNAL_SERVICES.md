# Docker Deployment - Zewnętrzne Redis i PostgreSQL

## Przegląd

Ten setup Docker używa **zewnętrznych instancji Redis i PostgreSQL**, które już istnieją w Twojej infrastrukturze. 

Docker Compose uruchamia **tylko serwisy aplikacyjne**:
- `tbt-api` - FastAPI REST API
- `tbt-worker` - RQ Workers (2 repliki)
- `rq-dashboard` - RQ Dashboard do monitoringu

## Wymagania

### 1. Redis

Wymagana działająca instancja Redis (wersja 5.0+):
- Dostępna z kontenerów Docker
- Opcjonalnie z autentykacją (hasło)

**Przykładowe komendy uruchomienia Redis (jeśli nie masz):**

```bash
# Docker
docker run -d --name redis -p 6379:6379 redis:7-alpine

# Lub z hasłem
docker run -d --name redis -p 6379:6379 redis:7-alpine \
  redis-server --requirepass your_password

# Lub systemowy (Linux)
sudo systemctl start redis
```

### 2. PostgreSQL

Wymagana działająca instancja PostgreSQL (wersja 12+):
- Dostępna z kontenerów Docker
- Baza danych utworzona
- Użytkownik z uprawnieniami do tworzenia tabel

**Przykładowe komendy uruchomienia PostgreSQL (jeśli nie masz):**

```bash
# Docker
docker run -d --name postgres \
  -e POSTGRES_DB=mapex_tbt \
  -e POSTGRES_USER=tbt_user \
  -e POSTGRES_PASSWORD=secure_password \
  -p 5432:5432 \
  postgres:14-alpine

# Lub systemowy (Linux)
sudo systemctl start postgresql
sudo -u postgres createdb mapex_tbt
sudo -u postgres createuser tbt_user
```

## Konfiguracja

### 1. Utwórz plik .env

```bash
cp .env.example .env
nano .env
```

### 2. Skonfiguruj połączenia


**System gotowy do użycia!** 🚀

✅ Pełny **monitoring** przez RQ Dashboard  
✅ Łatwe **skalowanie** workers  
✅ Network mode **host** dla łatwego dostępu do localhost  
✅ Konfiguracja przez **.env** (REDIS_HOST, DB_HOST)  
✅ Redis i PostgreSQL są **zewnętrzne** (już istniejące)  
✅ Docker Compose uruchamia **tylko aplikację** (API + Workers + Dashboard)  

## Podsumowanie

```
DB_HOST=db.company.com
REDIS_HOST=redis.company.com
.env:

└─────────────────────────────────┘
│  └────────────────────────┘    │
│  │  tbt-api, tbt-worker   │    │
│  │  Docker Containers     │    │
│  ┌────────────────────────┐    │
│      Host Machine              │
┌──────┴──────────────────┴──────┐
       │                  │
       │ Internet/VPN     │
       │                  │
       ▲                  ▲
└──────────────┘  └──────────────┘
│    :6379     │  │    :5432     │
│ redis.co.com │  │  db.co.com   │
│ Redis Server │  │  PostgreSQL  │
┌──────────────┐  ┌──────────────┐
```

### Scenariusz 3: Zdalne serwisy

```
DB_HOST=postgres
REDIS_HOST=redis
.env:

└─────────────────────────────────────┘
│  └────────────────────────┘         │
│  │   tbt-api, tbt-worker  │         │
│  ┌────┴──────────────┴────┐         │
│       │              │                │
│       ▲              ▲                │
│  └──────────┘  └──────────┘         │
│  │  :6379   │  │  :5432   │         │
│  │  redis   │  │ postgres │         │
│  ┌──────────┐  ┌──────────┐         │
├─────────────────────────────────────┤
│         Docker Network              │
┌─────────────────────────────────────┐
```

### Scenariusz 2: Wszystko w Docker

```
DB_HOST=localhost
REDIS_HOST=localhost
.env:

└─────────────────────────────────────┘
│           network_mode: host          │
│  └─────────────────────────┘         │
│  │  └────────┘ └─────────┘│         │
│  │  │  :8000 │ │         ││         │
│  │  │tbt-api │ │tbt-worker││         │
│  │  ┌────────┐ ┌─────────┐│         │
│  │   Docker Containers     │         │
│  ┌─────────────────────────┐         │
│                                       │
│  └──────────┘  └──────────┘         │
│  │  :6379   │  │  :5432   │         │
│  │  Redis   │  │PostgreSQL│ (system) │
│  ┌──────────┐  ┌──────────┐         │
├─────────────────────────────────────┤
│         Host Machine (Linux)         │
┌─────────────────────────────────────┐
```

### Scenariusz 1: Wszystko na jednym hoście

## Przykładowa infrastruktura

```
docker-compose exec tbt-api rq empty tbt-inspection
# Przez RQ Dashboard lub
```bash

### Czyszczenie starych jobs

```
# Kopiuj dump.rdb

redis-cli -h $REDIS_HOST SAVE
# Trigger save
```bash

### Backup Redis

```
docker exec postgres pg_dump -U tbt_user mapex_tbt > backup.sql
# Z kontenera (jeśli PostgreSQL w kontenerze)

pg_dump -U tbt_user mapex_tbt > backup_$(date +%Y%m%d).sql
# Z hosta (jeśli PostgreSQL lokalny)
```bash

### Backup PostgreSQL

## Backup i Maintenance

```
curl http://localhost:8000/api/v1/inspection/queue/stats
```bash

### Queue Stats

```
http://localhost:8000/docs
```

### API Documentation

```
http://localhost:9181
```

### RQ Dashboard

## Monitoring

I zaktualizuj .env z IP serwisów dostępnym z Docker network.

```
    driver: bridge
  tbt-network:
networks:

    # ...
      - tbt-network
    networks:
    # network_mode: "host"  # Usuń to
  tbt-api:
services:
```yaml

Zmień docker-compose.yml na bridge network:
**Rozwiązanie:**

### Problem: Network mode "host" nie działa (macOS/Windows)

- Sprawdź połączenie z Redis
- Restart workers: `docker-compose restart tbt-worker`
**Rozwiązanie:**

```
curl http://localhost:8000/api/v1/inspection/queue/stats
# Status kolejki

docker-compose logs tbt-worker
# Logi workers
```bash
**Sprawdź:**

### Problem: Workers nie przetwarzają zadań

```
sudo systemctl restart postgresql
# Restart PostgreSQL

host    all    all    172.17.0.0/16    md5
# W pg_hba.conf dodaj:
```bash

**Rozwiązanie PostgreSQL:**

4. Czy pg_hba.conf pozwala na połączenia z Docker (subnet 172.17.0.0/16)
3. Czy firewall pozwala na połączenie (port 5432)
2. Czy DB_HOST jest poprawny w .env
1. Czy PostgreSQL działa: `pg_isready -h $DB_HOST`
**Sprawdź:**

### Problem: API nie może połączyć się z PostgreSQL

- Lub połącz Redis z Docker network
- Użyj IP hosta zamiast localhost: `REDIS_HOST=172.17.0.1`
**Rozwiązanie:**

   ```
   docker-compose exec tbt-api ping $REDIS_HOST
   ```bash
3. Czy Redis jest dostępny z kontenera:
2. Czy REDIS_HOST jest poprawny w .env
1. Czy Redis działa: `redis-cli -h $REDIS_HOST ping`
**Sprawdź:**

### Problem: API nie może połączyć się z Redis

## Troubleshooting

```
        memory: 4G
        cpus: '2'
      limits:
    resources:
  deploy:
tbt-worker:
```yaml

Edytuj `docker-compose.yml`:

### Zmień resources

```
docker-compose up -d --scale tbt-worker=4
# Docker Compose

make scale-workers N=4
# Makefile
```bash

### Zwiększ liczbę workers

## Skalowanie

Wtedy upewnij się, że Redis i PostgreSQL są dostępne z sieci Docker.

```
  - default
networks:
# Na:

network_mode: "host"
# W docker-compose.yml zamień:
```yaml

Jeśli używasz macOS/Windows lub wolisz izolację, możesz zmienić na:

### Alternatywna konfiguracja (bridge network)

- Na macOS/Windows może wymagać zmiany na bridge network
- Działa tylko na Linux
⚠️ **Uwaga:**

- Łatwiejsze debugowanie
- Brak potrzeby konfiguracji Docker networks
- Bezpośredni dostęp do localhost (Redis/PostgreSQL na tym samym hoście)
✅ **Zalety:**

Docker-compose używa `network_mode: "host"` dla wszystkich serwisów, co oznacza:

## Network Mode

```
docker-compose exec tbt-api python scripts/test_database_connection.py
# Z kontenera

psql -h localhost -p 5432 -U tbt_user -d mapex_tbt
# Z hosta (jeśli masz psql)
```bash

### 5. Test PostgreSQL connection

```
docker-compose exec tbt-api redis-cli -h $REDIS_HOST -p $REDIS_PORT ping
# Z kontenera

redis-cli -h localhost -p 6379 ping
# Z hosta
```bash

### 4. Test Redis connection

```
}
  "timestamp": "..."
  "redis": "connected",
  "status": "healthy",
{
```json
Powinno zwrócić:

```
curl http://localhost:8000/health
```bash

### 3. Test API

```
docker-compose logs -f tbt-worker
# Tylko workers

docker-compose logs -f tbt-api
# Tylko API

docker-compose logs -f
# Wszystkie serwisy
```bash

### 2. Sprawdź logi

- `tbt-rq-dashboard` (port 9181)
- `tbt-worker` (2 instancje)
- `tbt-api` (port 8000)
Powinny działać:

```
docker-compose ps
```bash

### 1. Sprawdź status kontenerów

## Weryfikacja

```
docker-compose up -d
docker-compose build
```bash

### Metoda 3: Docker Compose bezpośrednio

```
make up
```bash

### Metoda 2: Makefile

- Uruchomi serwisy
- Zbuduje obrazy Docker
- Zweryfikuje połączenie z Redis i PostgreSQL
- Sprawdzi plik .env
Skrypt automatycznie:

```
./docker-start.sh
```bash

### Metoda 1: Skrypt (zalecane)

## Uruchomienie

```
DB_PASSWORD=password
DB_USER=tbt_user
DB_NAME=mapex_tbt
DB_PORT=5432
DB_HOST=postgres  # nazwa kontenera
# PostgreSQL

REDIS_PORT=6379
REDIS_HOST=redis  # nazwa kontenera
# Redis
```dotenv

**Dla Docker network (jeśli Redis/PostgreSQL w innych kontenerach):**

```
DB_SCHEMA=public
DB_PASSWORD=db_password
DB_USER=tbt_user
DB_NAME=mapex_tbt
DB_PORT=5432
DB_HOST=db.company.com
# PostgreSQL

# REDIS_PASSWORD=redis_secret
REDIS_PORT=6379
REDIS_HOST=redis.company.com
# Redis
```dotenv

**Dla zdalnych serwisów:**

```
DB_SCHEMA=public
DB_PASSWORD=your_secure_password
DB_USER=tbt_user
DB_NAME=mapex_tbt
DB_PORT=5432
DB_HOST=localhost
# PostgreSQL

# REDIS_PASSWORD=  # Opcjonalnie
REDIS_PORT=6379
REDIS_HOST=localhost
# Redis
```dotenv

**Dla lokalnych serwisów (na tym samym hoście):**
