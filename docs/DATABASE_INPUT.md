# Odczytywanie danych z bazy danych

## Przegląd

System został zmodyfikowany, aby odczytywać dane tras z lokalnej bazy danych PostgreSQL zamiast z plików GeoJSON. Wszystkie dane są pobierane z tabeli `pipeline_routes` na podstawie `pipeline_id`.

## Struktura tabeli

```sql
CREATE TABLE tbt.pipeline_routes (
    id SERIAL PRIMARY KEY,
    pipeline_id UUID NOT NULL,
    route_data JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
```

### Format danych w kolumnie `route_data`

```json
{
    "org": "genesis",
    "name": "Dhaka Kebab - MN Boxing (neg)",
    "origin": "POINT(22.50216 52.91886)",
    "country": "POL",
    "quality": "Q2",
    "tile_id": "13_4608_2671",
    "route_id": "9f81db8f-b0aa-4b74-a6c0-99ab5174339f",
    "to_coord": "52.91667, 22.51747",
    "sample_id": "0273e3cc-a095-4e4a-aa33-b760433ed8fe",
    "date_generated": "2025-12-18",
    "from_coord": "52.91886, 22.50216",
    "length_orbis": "24684.0",
    "length_genesis": "23894.0"
}
```

## Konfiguracja

Dane uwierzytelniające do bazy danych są przechowywane w pliku `.env`:

```env
DB_HOST=psql-litools-flexdb-pg16-prod-weu.postgres.database.azure.com
DB_PORT=5432
DB_NAME=routeexplorer
DB_USER=ecaadmin
DB_PASSWORD=Msoliadmin123!
DB_SCHEMA=tbt
```

## Użycie API

### Poprzednio (z plikiem GeoJSON):
```bash
curl -X POST http://localhost:8000/api/v1/inspection/submit \
  -F "geojson_file=@Routes2check.geojson" \
  -F "provider=Orbis" \
  -F "competitor=Genesis"
```

### Teraz (z pipeline_id):
```bash
curl -X POST http://localhost:8000/api/v1/inspection/submit \
  -F "pipeline_id=b294bb07-b9d6-4e6f-8100-b909fe6227df" \
  -F "provider=Orbis" \
  -F "competitor=Genesis"
```

## Użycie skryptu testowego

### Poprzednio:
```bash
python scripts/test_api.py li_input/geojson/Routes2check.geojson
```

### Teraz:
```bash
python scripts/test_api.py b294bb07-b9d6-4e6f-8100-b909fe6227df
```

## Przepływ danych

1. **API otrzymuje `pipeline_id`** w żądaniu POST do `/api/v1/inspection/submit`
2. **Worker (RQ)** otrzymuje job z parametrem `pipeline_id`
3. **Pipeline Kedro** przekazuje `pipeline_id` w `tbt_options`
4. **Node `initialize_sampling_data`** wykrywa `pipeline_id` i uruchamia skrypt `generate_sampling_from_db.py`
5. **Skrypt `generate_sampling_from_db.py`**:
   - Łączy się z bazą danych używając credentials z `.env`
   - Pobiera wszystkie rekordy z `tbt.pipeline_routes` WHERE `pipeline_id = ?`
   - Konwertuje `route_data` (JSONB) do formatu GeoJSON
   - Generuje pliki parquet: `sampling_samples.parquet` i `sampling_metadata.parquet`
6. **Pipeline kontynuuje** standardowe przetwarzanie używając wygenerowanych plików parquet

## Fallback do GeoJSON

Jeśli `pipeline_id` nie jest podany w `tbt_options`, system automatycznie wraca do starego sposobu działania i używa pliku `li_input/geojson/Routes2check.geojson`.

## Zmiany w plikach

### Nowe pliki:
- `scripts/generate_sampling_from_db.py` - Skrypt pobierający dane z bazy i generujący pliki parquet

### Zmodyfikowane pliki:
- `src/tbt_api/main.py` - Endpoint przyjmuje `pipeline_id` zamiast pliku GeoJSON
- `src/tbt_api/worker.py` - Worker przekazuje `pipeline_id` do pipeline'u
- `src/tbt/pipelines/inspection/nodes/initialize_sampling.py` - Obsługa `pipeline_id` z fallbackiem do GeoJSON
- `src/tbt/pipelines/inspection/pipelines/pre_inspection.py` - Przekazywanie `tbt_options` do node'a
- `scripts/test_api.py` - Zaktualizowany do używania `pipeline_id`

## Testowanie

### Testowanie lokalne z bazą danych:

```python
# scripts/generate_sampling_from_db.py
python scripts/generate_sampling_from_db.py b294bb07-b9d6-4e6f-8100-b909fe6227df
```

Powinno wygenerować:
- `data/tbt/sampling/sampling_samples.parquet`
- `data/tbt/sampling/sampling_metadata.parquet`

### Testowanie API:

1. Upewnij się, że Redis działa i jest dostępny
2. Uruchom API: `python src/tbt_api/main.py`
3. Uruchom worker: `rq worker tbt-inspection --url redis://localhost:6379`
4. Wyślij żądanie z `pipeline_id`

## Troubleshooting

### Błąd połączenia z bazą danych
- Sprawdź credentials w `.env`
- Sprawdź czy masz dostęp do bazy danych (firewall, VPN)
- Sprawdź czy tabela `tbt.pipeline_routes` istnieje

### Brak danych dla pipeline_id
```
ValueError: No route data found for pipeline_id: xxx
```
- Sprawdź czy `pipeline_id` istnieje w tabeli
- Sprawdź czy są jakieś rekordy dla tego `pipeline_id`

### Błąd parsowania route_data
- Sprawdź format JSONB w bazie danych
- Upewnij się, że wszystkie wymagane pola są obecne: `origin`, `to_coord`, `sample_id`, `route_id`, etc.

