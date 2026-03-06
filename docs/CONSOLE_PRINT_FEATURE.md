# Console Print Feature

## Opis

W projekcie TbT została dodana funkcjonalność warunkowego wyświetlania wiadomości za pomocą `print()` obok standardowego logowania przez `log.info()`. Ta funkcjonalność umożliwia wyświetlanie wiadomości bezpośrednio w konsoli, co może być przydatne podczas debugowania lub monitorowania pipeline'u w czasie rzeczywistym.

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

- **`enable_console_prints: True`** - Włącza wyświetlanie wiadomości przez `print()` obok `log.info()`
- **`enable_console_prints: False`** - Wyłącza wyświetlanie wiadomości przez `print()`, pozostawiając tylko standardowe logowanie

## Implementacja

### Moduł pomocniczy

Utworzono moduł `src/tbt/utils/console_print.py` zawierający funkcję `conditional_print()`:

```python
from tbt.utils.console_print import conditional_print

# Użycie w kodzie
log.info("Processing %d items", count)
conditional_print("Processing %d items", count)
```

Funkcja `conditional_print()` sprawdza ustawienie `enable_console_prints` w pliku konfiguracyjnym i wyświetla wiadomość tylko wtedy, gdy jest włączone.

### Zmodyfikowane pliki

Funkcjonalność została dodana do wszystkich węzłów (nodes) w projekcie:

#### Węzły inspection
- `src/tbt/pipelines/inspection/nodes/initialize.py`
- `src/tbt/pipelines/inspection/nodes/initialize_sampling.py`
- `src/tbt/pipelines/inspection/nodes/cleanup.py`
- `src/tbt/pipelines/inspection/nodes/duplicates.py`
- `src/tbt/pipelines/inspection/nodes/routing.py`
- `src/tbt/pipelines/inspection/nodes/reuse.py`
- `src/tbt/pipelines/inspection/nodes/rac.py`
- `src/tbt/pipelines/inspection/nodes/fcd.py`
- `src/tbt/pipelines/inspection/nodes/merge.py`
- `src/tbt/pipelines/inspection/nodes/sanity.py`
- `src/tbt/pipelines/inspection/nodes/export.py`

#### Moduły ecmodel
- `src/tbt/ecmodel/error_classification/ec_end2end.py`
- `src/tbt/ecmodel/error_classification/call_SDO_API.py`

#### Moduły navutils
- `src/tbt/navutils/navutils/sanity_checks.py`
- `src/tbt/navutils/navutils/mmi.py`

## Przykłady użycia

### Przykład 1: Proste wiadomości

```python
log.info("Starting process...")
conditional_print("Starting process...")
```

### Przykład 2: Wiadomości z parametrami

```python
log.info("Computed %i routes for provider %s", count, provider_name)
conditional_print("Computed %i routes for provider %s", count, provider_name)
```

### Przykład 3: Wiadomości z formatowaniem f-string

```python
log.info(f"Processing {item_count} items from {source}")
conditional_print(f"Processing {item_count} items from {source}")
```

## Zalety

1. **Elastyczność** - Możliwość łatwego włączania/wyłączania print() bez modyfikacji kodu
2. **Debugowanie** - Łatwiejsze śledzenie wykonania pipeline'u w czasie rzeczywistym
3. **Kompatybilność wsteczna** - Standardowe logowanie działa niezależnie od ustawienia console prints
4. **Centralna konfiguracja** - Jedno miejsce do kontrolowania wyświetlania wiadomości w całym projekcie

## Uwagi

- Wiadomości wyświetlane przez `print()` nie są zapisywane do plików logów
- Zmiana konfiguracji `enable_console_prints` wymaga ponownego uruchomienia pipeline'u
- Funkcja `conditional_print()` jest thread-safe i może być używana w środowisku wielowątkowym
- W przypadku problemów z ładowaniem konfiguracji, domyślnie `enable_console_prints` jest ustawione na `True`

## Rozwiązywanie problemów

### Problem: Wiadomości nie są wyświetlane
- Sprawdź czy `enable_console_prints: True` w `conf/base/logging.yml`
- Upewnij się, że plik konfiguracyjny jest dostępny
- Zrestartuj pipeline

### Problem: Zbyt wiele wiadomości w konsoli
- Ustaw `enable_console_prints: False` w `conf/base/logging.yml`
- Możesz również skomentować lub usunąć wywołania `conditional_print()` w konkretnych węzłach

## Przyszłe ulepszenia

Potencjalne ulepszenia tej funkcjonalności:
- Dodanie poziomów wyświetlania (DEBUG, INFO, WARNING, ERROR)
- Możliwość konfiguracji per węzeł/moduł
- Kolorowanie wiadomości w konsoli
- Kierowanie wiadomości do różnych strumieni wyjściowych

