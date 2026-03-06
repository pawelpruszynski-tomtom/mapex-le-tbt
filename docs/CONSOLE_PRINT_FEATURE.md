# Console Print Feature

## Opis

W projekcie TbT została dodana funkcjonalność warunkowego wyświetlania wiadomości za pomocą `print()` obok standardowego logowania. Ta funkcjonalność umożliwia wyświetlanie wiadomości bezpośrednio w konsoli na wszystkich poziomach logowania (INFO, WARNING, ERROR, DEBUG, CRITICAL), co może być przydatne podczas debugowania lub monitorowania pipeline'u w czasie rzeczywistym.

## Konfiguracja

Wyświetlanie wiadomości przez `print()` jest kontrolowane przez konfigurację w pliku `conf/base/logging.yml`:

```yaml
version: 1

disable_existing_loggers: False

# Enable or disable console print statements alongside log.info
enable_console_prints: True

formatters:
  simple:
    format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
...
```

### Opcje konfiguracji

- **`enable_console_prints: True`** - Włącza wyświetlanie wiadomości przez `print()` obok loggera
- **`enable_console_prints: False`** - Wyłącza wyświetlanie wiadomości przez `print()`, pozostawiając tylko standardowe logowanie

## Implementacja

### Moduł pomocniczy

Utworzono moduł `src/tbt/utils/console_print.py` zawierający następujące funkcje:

```python
from tbt.utils.console_print import (
    conditional_print,           # INFO level
    conditional_print_warning,   # WARNING level
    conditional_print_error,     # ERROR level
    conditional_print_debug,     # DEBUG level
    conditional_print_critical   # CRITICAL level
)

# Użycie w kodzie
log.info("Processing %d items", count)
conditional_print("Processing %d items", count)

log.warning("High resource usage: %d%%", usage)
conditional_print_warning("High resource usage: %d%%", usage)

log.error("Failed to process: %s", error_msg)
conditional_print_error("Failed to process: %s", error_msg)
```

Każda funkcja `conditional_print_*()` sprawdza ustawienie `enable_console_prints` w pliku konfiguracyjnym i wyświetla wiadomość tylko wtedy, gdy jest włączone. Dodatkowo, wiadomości są poprzedzone odpowiednim prefiksem poziomu (np. `[WARNING]`, `[ERROR]`).

### Dostępne funkcje

| Funkcja | Poziom logowania | Prefix w konsoli |
|---------|-----------------|------------------|
| `conditional_print()` | INFO | `[INFO]` |
| `conditional_print_warning()` | WARNING | `[WARNING]` |
| `conditional_print_error()` | ERROR | `[ERROR]` |
| `conditional_print_debug()` | DEBUG | `[DEBUG]` |
| `conditional_print_critical()` | CRITICAL | `[CRITICAL]` |

### Zmodyfikowane pliki

Funkcjonalność została dodana do wszystkich węzłów (nodes) w projekcie:

#### Węzły inspection
- `src/tbt/pipelines/inspection/nodes/initialize.py` (INFO, WARNING)
- `src/tbt/pipelines/inspection/nodes/initialize_sampling.py` (INFO, WARNING)
- `src/tbt/pipelines/inspection/nodes/cleanup.py` (INFO, WARNING)
- `src/tbt/pipelines/inspection/nodes/duplicates.py` (INFO)
- `src/tbt/pipelines/inspection/nodes/routing.py` (INFO)
- `src/tbt/pipelines/inspection/nodes/reuse.py` (INFO)
- `src/tbt/pipelines/inspection/nodes/rac.py` (INFO)
- `src/tbt/pipelines/inspection/nodes/fcd.py` (INFO)
- `src/tbt/pipelines/inspection/nodes/merge.py` (INFO)
- `src/tbt/pipelines/inspection/nodes/sanity.py` (INFO)
- `src/tbt/pipelines/inspection/nodes/export.py` (INFO, ERROR)

#### Moduły ecmodel
- `src/tbt/ecmodel/error_classification/ec_end2end.py` (INFO, WARNING)
- `src/tbt/ecmodel/error_classification/call_SDO_API.py` (INFO, ERROR)
- `src/tbt/ecmodel/error_classification/featurization.py` (WARNING)

#### Moduły navutils
- `src/tbt/navutils/navutils/sanity_checks.py` (INFO, ERROR)
- `src/tbt/navutils/navutils/mmi.py` (INFO)

## Przykłady użycia

### Przykład 1: INFO - Proste wiadomości

```python
log.info("Starting process...")
conditional_print("Starting process...")
# Output: [INFO] Starting process...
```

### Przykład 2: INFO - Wiadomości z parametrami

```python
log.info("Computed %i routes for provider %s", count, provider_name)
conditional_print("Computed %i routes for provider %s", count, provider_name)
# Output: [INFO] Computed 42 routes for provider TomTom
```

### Przykład 3: WARNING - Ostrzeżenia

```python
log.warning("Directory does not exist, skipping: %s", dir_path)
conditional_print_warning("Directory does not exist, skipping: %s", dir_path)
# Output: [WARNING] Directory does not exist, skipping: /path/to/dir
```

### Przykład 4: ERROR - Błędy

```python
log.error(f"Failed to export data to database: {str(e)}")
conditional_print_error(f"Failed to export data to database: {str(e)}")
# Output: [ERROR] Failed to export data to database: Connection timeout
```

### Przykład 5: DEBUG - Debugowanie

```python
log.debug("Variable value: %s", debug_var)
conditional_print_debug("Variable value: %s", debug_var)
# Output: [DEBUG] Variable value: sample_value
```

### Przykład 6: CRITICAL - Błędy krytyczne

```python
log.critical("System failure: %s", critical_error)
conditional_print_critical("System failure: %s", critical_error)
# Output: [CRITICAL] System failure: Out of memory
```

## Zalety

1. **Elastyczność** - Możliwość łatwego włączania/wyłączania print() bez modyfikacji kodu
2. **Wszystkie poziomy** - Obsługa wszystkich poziomów logowania (INFO, WARNING, ERROR, DEBUG, CRITICAL)
3. **Widoczność poziomu** - Każda wiadomość jest poprzedzona prefiksem poziomu logowania
4. **Debugowanie** - Łatwiejsze śledzenie wykonania pipeline'u w czasie rzeczywistym
5. **Kompatybilność wsteczna** - Standardowe logowanie działa niezależnie od ustawienia console prints
6. **Centralna konfiguracja** - Jedno miejsce do kontrolowania wyświetlania wiadomości w całym projekcie

## Statystyki pokrycia

Łącznie dodano warunkowy print dla:
- **INFO**: ~50 miejsc
- **WARNING**: ~7 miejsc
- **ERROR**: ~6 miejsc
- **DEBUG**: 0 miejsc (brak użyć w kodzie)
- **CRITICAL**: 0 miejsc (brak użyć w kodzie)

## Uwagi

- Wiadomości wyświetlane przez `print()` nie są zapisywane do plików logów
- Zmiana konfiguracji `enable_console_prints` wymaga ponownego uruchomienia pipeline'u
- Funkcje `conditional_print_*()` są thread-safe i mogą być używane w środowisku wielowątkowym
- W przypadku problemów z ładowaniem konfiguracji, domyślnie `enable_console_prints` jest ustawione na `True`
- Każdy poziom logowania ma swój własny prefix dla łatwiejszej identyfikacji w konsoli

## Rozwiązywanie problemów

### Problem: Wiadomości nie są wyświetlane
- Sprawdź czy `enable_console_prints: True` w `conf/base/logging.yml`
- Upewnij się, że plik konfiguracyjny jest dostępny
- Zrestartuj pipeline

### Problem: Zbyt wiele wiadomości w konsoli
- Ustaw `enable_console_prints: False` w `conf/base/logging.yml`
- Możesz również skomentować lub usunąć wywołania `conditional_print_*()` w konkretnych węzłach

### Problem: Brak prefiksu poziomu
- Sprawdź czy używasz odpowiedniej funkcji (`conditional_print_warning()` zamiast `conditional_print()` dla WARNING)

## Przyszłe ulepszenia

Potencjalne ulepszenia tej funkcjonalności:
- Dodanie możliwości konfiguracji per węzeł/moduł
- Kolorowanie wiadomości w konsoli w zależności od poziomu
- Kierowanie wiadomości do różnych strumieni wyjściowych
- Oddzielna konfiguracja dla każdego poziomu logowania
- Timestamp w wiadomościach console print

