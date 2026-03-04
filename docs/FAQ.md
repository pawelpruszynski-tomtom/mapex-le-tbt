# FAQ - Najczęściej zadawane pytania

## Użycie API

### Gdzie musi znajdować się plik GeoJSON przy wywołaniu curl?

**Krótka odpowiedź:** Plik może być **wszędzie**, ale ścieżka w curl musi być **poprawna względem miejsca, z którego wywołujesz curl**.

**Przykłady:**

#### 1. GeoJSON w tym samym katalogu co curl
```bash
cd /home/user/my_files
curl -X POST http://localhost:8000/api/v1/inspection/submit \
  -F "geojson_file=@routes.geojson" \
  -F "provider=Orbis"
```

#### 2. GeoJSON w podkatalogu
```bash
# Będąc w /home/user/mapex-le-tbt
curl -X POST http://localhost:8000/api/v1/inspection/submit \
  -F "geojson_file=@li_input/geojson/Routes2check.geojson" \
  -F "provider=Orbis"
```

#### 3. GeoJSON ze ścieżką absolutną (działa z dowolnego miejsca)
```bash
# Możesz być w dowolnym katalogu
curl -X POST http://localhost:8000/api/v1/inspection/submit \
  -F "geojson_file=@/home/user/mapex-le-tbt/li_input/geojson/Routes2check.geojson" \
  -F "provider=Orbis"
```

#### 4. GeoJSON w katalogu wyżej
```bash
# Będąc w /home/user/mapex-le-tbt/scripts
curl -X POST http://localhost:8000/api/v1/inspection/submit \
  -F "geojson_file=@../li_input/geojson/Routes2check.geojson" \
  -F "provider=Orbis"
```

**Wskazówka:** Użyj **ścieżki absolutnej** jeśli nie jesteś pewien - zawsze zadziała!

### Co oznacza @ w curl?

Symbol `@` w curl oznacza "przeczytaj zawartość pliku i wyślij jako część formularza".

```bash
# @ = czytaj z pliku
-F "geojson_file=@routes.geojson"

# Bez @ = wyślij string "routes.geojson"
-F "geojson_file=routes.geojson"  # ❌ Błąd!
```

### Jak sprawdzić czy plik istnieje przed wysłaniem?

```bash
# Linux/macOS/WSL
FILE="li_input/geojson/Routes2check.geojson"
if [ -f "$FILE" ]; then
    echo "✅ Plik istnieje: $FILE"
    curl -X POST http://localhost:8000/api/v1/inspection/submit \
      -F "geojson_file=@$FILE" \
      -F "provider=Orbis"
else
    echo "❌ Plik nie istnieje: $FILE"
fi
```

### Czy mogę wysłać plik z Windowsa?

Tak! Użyj ścieżki Windows lub WSL:

```bash
# PowerShell (Windows)
curl -X POST http://localhost:8000/api/v1/inspection/submit `
  -F "geojson_file=@C:\Users\user\routes.geojson" `
  -F "provider=Orbis"

# WSL (dostęp do dysków Windows)
curl -X POST http://localhost:8000/api/v1/inspection/submit \
  -F "geojson_file=@/mnt/c/Users/user/routes.geojson" \
  -F "provider=Orbis"

# Git Bash (Windows)
curl -X POST http://localhost:8000/api/v1/inspection/submit \
  -F "geojson_file=@/c/Users/user/routes.geojson" \
  -F "provider=Orbis"
```

## Problemy z plikami

### Błąd: "No such file or directory"

```
curl: (26) Failed to open/read local data from file/application
```

**Przyczyna:** curl nie może znaleźć pliku.

**Rozwiązanie:**
```bash
# 1. Sprawdź obecny katalog
pwd

# 2. Sprawdź czy plik istnieje
ls -la routes.geojson
# lub
ls -la li_input/geojson/Routes2check.geojson

# 3. Użyj ścieżki absolutnej
curl -X POST http://localhost:8000/api/v1/inspection/submit \
  -F "geojson_file=@$(pwd)/routes.geojson" \
  -F "provider=Orbis"
```

### Błąd: "File must be a GeoJSON (.geojson or .json)"

**Przyczyna:** API akceptuje tylko pliki z rozszerzeniem `.geojson` lub `.json`.

**Rozwiązanie:**
```bash
# Zmień nazwę pliku
mv routes.txt routes.geojson

# Lub użyj .json
mv routes.txt routes.json
```

### Plik jest za duży

API może mieć limit rozmiaru pliku (zazwyczaj do kilkudziesięciu MB).

**Rozwiązanie:**
- Podziel GeoJSON na mniejsze pliki
- Wyślij jako osobne zadania
- Lub zwiększ limit w konfiguracji FastAPI (plik `tbt_api/main.py`)

## Monitoring i debugowanie

### Jak sprawdzić czy API działa?

```bash
# Health check
curl http://localhost:8000/health

# Oczekiwana odpowiedź:
# {"status":"healthy","redis":"connected","timestamp":"..."}
```

### Jak zobaczyć logi zadania?

```bash
# Logi API
docker-compose logs -f tbt-api

# Logi workers
docker-compose logs -f tbt-worker

# Wszystkie logi
docker-compose logs -f
```

### Jak sprawdzić status kolejki?

```bash
# Przez API
curl http://localhost:8000/api/v1/inspection/queue/stats

# Przez RQ Dashboard
# Otwórz w przeglądarce: http://localhost:9181
```

### Jak anulować zadanie?

```bash
# Pobierz job_id z odpowiedzi submit
curl -X DELETE http://localhost:8000/api/v1/inspection/cancel/{job_id}
```

## Konfiguracja

### Gdzie są credentiale do Redis i PostgreSQL?

W pliku `.env` w głównym katalogu projektu:

```bash
# Edytuj
nano .env

# Lub sprawdź
cat .env
```

### Jak zmienić liczbę workers?

```bash
# Przez Makefile
make scale-workers N=4

# Przez docker-compose
docker-compose up -d --scale tbt-worker=4

# Lub edytuj docker-compose.yml:
# tbt-worker:
#   deploy:
#     replicas: 4
```

### Jak zmienić port API?

Edytuj `docker-compose.yml`:

```yaml
tbt-api:
  command: uvicorn tbt_api.main:app --host 0.0.0.0 --port 9000  # Zmień 8000 -> 9000
```

**Uwaga:** Z `network_mode: host` port jest dostępny bezpośrednio na hoście.

## Python Client

### Jak używać Python client zamiast curl?

```python
from tbt_api.client import TbtInspectionClient

client = TbtInspectionClient()

# Submit
result = client.submit_and_wait(
    geojson_file="routes.geojson",  # Ścieżka względem skryptu Python
    provider="Orbis",
    competitor="Genesis"
)

print(result)
```

### Jak używać test script?

```bash
# Podstawowe użycie
python3 scripts/test_api.py routes.geojson

# Z verbose output
python3 scripts/test_api.py routes.geojson --verbose

# Help
python3 scripts/test_api.py --help
```

## Docker

### Jak zrestartować tylko API bez workers?

```bash
docker-compose restart tbt-api
```

### Jak przebudować obraz po zmianach w kodzie?

```bash
# Rebuild bez cache
docker-compose build --no-cache

# Lub tylko API
docker-compose build --no-cache tbt-api

# Restart po rebuild
docker-compose up -d
```

### Jak całkowicie wyczyścić i zacząć od nowa?

```bash
# Stop i usuń wszystko
docker-compose down -v

# Usuń obrazy
docker rmi tbt-inspection:latest

# Rebuild
docker-compose build --no-cache

# Start
docker-compose up -d
```

### Jak sprawdzić logi błędów podczas build?

```bash
# Build z verbose output
docker-compose build --progress=plain

# Lub bezpośrednio
docker build --progress=plain -t tbt-inspection:latest .
```

## Troubleshooting

### API nie odpowiada

```bash
# 1. Sprawdź czy kontener działa
docker-compose ps

# 2. Sprawdź logi
docker-compose logs tbt-api

# 3. Restart
docker-compose restart tbt-api

# 4. Health check
curl http://localhost:8000/health
```

### Workers nie przetwarzają zadań

```bash
# 1. Sprawdź logi workers
docker-compose logs tbt-worker

# 2. Sprawdź połączenie z Redis
docker-compose exec tbt-api redis-cli -h $REDIS_HOST ping

# 3. Restart workers
docker-compose restart tbt-worker
```

### Nie mogę połączyć się z Redis/PostgreSQL

```bash
# 1. Sprawdź czy serwisy działają
redis-cli -h localhost ping
pg_isready -h localhost

# 2. Sprawdź .env
cat .env | grep -E "REDIS_HOST|DB_HOST"

# 3. Test z kontenera
docker-compose exec tbt-api ping $REDIS_HOST
```

### Zadanie jest w kolejce ale się nie wykonuje

```bash
# 1. Sprawdź RQ Dashboard
# http://localhost:9181

# 2. Sprawdź ilość workers
docker-compose ps | grep tbt-worker

# 3. Sprawdź kolejkę
curl http://localhost:8000/api/v1/inspection/queue/stats

# 4. Restart workers
docker-compose restart tbt-worker
```

## Wydajność

### Jak przyspieszyć przetwarzanie?

```bash
# Zwiększ liczbę workers
make scale-workers N=4

# Lub bezpośrednio
docker-compose up -d --scale tbt-worker=4
```

### Ile zadań może przetwarzać jednocześnie?

Liczba równoległych zadań = liczba workers (domyślnie 2).

Każdy worker przetwarza **jedno zadanie naraz**.

### Jak monitorować zużycie zasobów?

```bash
# CPU i RAM
docker stats

# Tylko API i workers
docker stats tbt-api tbt-worker
```

## Bezpieczeństwo

### Czy mogę wyłączyć autentykację API?

API obecnie **nie ma autentykacji**. W produkcji dodaj:
- JWT tokens
- API keys
- OAuth2

Zobacz: `tbt_api/main.py` dla dodania middleware.

### Czy .env jest bezpieczny?

Plik `.env` jest w `.gitignore` - nie zostanie scommitowany.

**Ważne:**
- ✅ `.env` - lokalne credentials (git ignore)
- ✅ `.env.example` - szablon bez credentials (w git)
- ❌ NIE commituj `.env` do git!

### Jak zabezpieczyć API w produkcji?

1. Dodaj autentykację (JWT/API keys)
2. Użyj HTTPS (nginx reverse proxy)
3. Ogranicz dostęp przez firewall
4. Rate limiting
5. Walidacja inputów

## Więcej pytań?

- 📖 Dokumentacja: `docs/DOCKER_DEPLOYMENT.md`
- 📖 Zewnętrzne serwisy: `docs/DOCKER_EXTERNAL_SERVICES.md`
- 📖 Network mode: `docs/NETWORK_MODE_HOST.md`
- 🐛 Issues: Sprawdź logi (`docker-compose logs`)
- 💡 Przykłady: `examples/api_client_usage.py`

