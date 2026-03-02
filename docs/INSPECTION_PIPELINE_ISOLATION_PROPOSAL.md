# Propozycja wyizolowania pipeline'u `tbt_inspection`

## 1. Analiza stanu obecnego

### 1.1 Struktura projektu

Projekt `analytics_tbt` to monolityczny projekt Kedro (v0.18.4) zawierający **8 pipeline'ów** (z czego tylko `tbt_inspection` jest aktywny — reszta zakomentowana w `pipeline_registry.py`):

```
src/tbt/
├── pipelines/
│   ├── inspection/          # <-- AKTYWNY PIPELINE
│   │   ├── __init__.py
│   │   ├── pipeline.py      # 201 linii - definicja 9 node'ów
│   │   ├── nodes.py         # 1490 linii - cała logika biznesowa
│   │   └── integration.py   # 67 linii - testy integracyjne
│   ├── sampling/            # zakomentowany
│   ├── sampling_hdr/        # zakomentowany
│   ├── metric_calculation/  # zakomentowany
│   ├── split_region/        # zakomentowany
│   ├── delete/              # zakomentowany
│   ├── aggregation_ww/      # zakomentowany
│   └── rocco/               # zakomentowany
├── navutils/                # git submodule - Provider, RAC, sanity checks
├── ecmodel/                 # git submodule - Error Classification ML model
├── pipeline_registry.py
├── settings.py
└── __main__.py
```

### 1.2 Pipeline `tbt_inspection` — flow danych

```
                                   ┌─────────────────────────────────────┐
                                   │         DANE WEJŚCIOWE (Spark)      │
                                   │  - tbt_sampling_samples_sdf         │
                                   │  - tbt_sampling_metadata_sdf        │
                                   │  - tbt_inspection_routes_input      │
                                   │  - tbt_inspection_critical_*_input  │
                                   │  - tbt_inspection_metadata_input    │
                                   └──────────────┬──────────────────────┘
                                                  │
                    ┌─────────────────────────────┼──────────────────────────────┐
                    ▼                             ▼                              │
        ┌──────────────────────┐   ┌──────────────────────────┐                 │
        │ get_provider_routes  │   │   reuse_static_routes    │                 │
        │ (API: Orbis/inne)    │──▶│ (porównanie z historią)   │                 │
        └──────────────────────┘   └──────────┬───────────────┘                 │
                                              │                                 │
                         ┌────────────────────┼──────────────────┐              │
                         ▼                    ▼                  ▼              │
              ┌────────────────────┐  ┌──────────────┐  ┌──────────────┐       │
              │get_competitor_routes│  │ known routes │  │unknown routes│       │
              │(API: Genesis/inne) │  └──────────────┘  └──────────────┘       │
              └────────┬───────────┘                                           │
                       ▼                                                       │
              ┌────────────────────┐                                           │
              │   get_rac_state    │ RAC: Route Assessment Classification      │
              │   (critical        │                                           │
              │    sections)       │                                           │
              └────────┬───────────┘                                           │
                       ▼                                                       │
              ┌────────────────────┐                                           │
              │   get_fcd_state    │ FCD + ML model (ecmodel submodule)        │
              └────────┬───────────┘                                           │
                       ▼                                                       │
              ┌────────────────────────┐                                       │
              │ merge_inspection_data  │◀──────────────────────────────────────┘
              └────────┬───────────────┘
                       ▼
              ┌────────────────────┐
              │   sanity_check     │
              └────────┬───────────┘
                       ▼
              ┌───────────────────┐   ┌──────────────────┐
              │  export_to_csv    │   │  export_to_spark  │
              └───────────────────┘   └──────────────────┘
                       ▼
              ┌────────────────────┐
              │ raise_sanity_error │
              └────────────────────┘
```

### 1.3 Zależności zewnętrzne pipeline'u `inspection`

| Zależność | Typ | Opis |
|-----------|-----|------|
| `tbt.navutils` | git submodule | `Provider`, `CriticalSectionIteration` (RAC), `SanityCheckInspectionTBT`, `decorators` |
| `tbt.ecmodel` | git submodule | `ErrorClassification` — model ML do klasyfikacji błędów |
| `pyspark` (3.3.2) | pip package | Silnik obliczeniowy (SparkDataFrame, RDD) |
| `shapely` (2.0) | pip package | Operacje geometryczne na trasach |
| `kedro` (0.18.4) | pip package | Orkiestracja pipeline'u |

### 1.4 Zidentyfikowane problemy

1. **Monolityczny plik `nodes.py` (1490 linii)** — cała logika w jednym pliku
2. **Silne sprzężenie z Kedro** — logika biznesowa jest przemieszana z orchestracją
3. **Silne sprzężenie ze Spark** — logika routingu/RAC mogłaby działać bez Spark
4. **Zakomentowane pipeline'y** — 7 z 8 pipeline'ów jest zakomentowanych, ale kod nadal istnieje
5. **Git submoduły** — `navutils` i `ecmodel` to osobne repozytoria, które komplikują zarządzanie zależnościami
6. **Brak separacji warstw** — API calls, transformacje danych, i eksport w jednym miejscu
7. **Stara wersja Kedro** (0.18.4) — dwa major releases za obecną (0.19.x)
8. **Brak punktów rozszerzenia** — nie ma jasnego mechanizmu dodawania kroków pre/post (np. zapis do bazy danych, walidacja wstępna, notyfikacje)

---

## 2. Propozycje wyizolowania

### Opcja A: Refaktoryzacja wewnętrzna (niski koszt, szybka) ★ REKOMENDOWANA JAKO FAZA 1

Reorganizacja kodu wewnątrz obecnego projektu Kedro **bez zmiany frameworka**, z architekturą przygotowaną na rozszerzenia.

#### 2.1 Struktura docelowa:

```
src/tbt/pipelines/inspection/
├── __init__.py
├── pipeline.py                    # ZMIANA: kompozycja sub-pipeline'ów
├── nodes/                         # ZMIANA: rozbicie nodes.py na moduły
│   ├── __init__.py                # re-eksport wszystkich node functions
│   ├── routing.py                 # get_provider_routes, get_competitor_routes
│   ├── reuse.py                   # reuse_static_routes
│   ├── rac.py                     # get_rac_state
│   ├── fcd.py                     # get_fcd_state, evaluate_with_ml_model
│   ├── merge.py                   # merge_inspection_data
│   ├── sanity.py                  # sanity_check, raise_sanity_error
│   ├── export.py                  # export_to_csv, export_to_spark, export_to_sql
│   └── duplicates.py              # check_duplicates
├── pipelines/                     # NOWE: sub-pipeline'y (punkty rozszerzenia)
│   ├── __init__.py
│   ├── pre_inspection.py          # pipeline: walidacja, dedup, przygotowanie danych
│   ├── core_inspection.py         # pipeline: routing → RAC → FCD → merge
│   └── post_inspection.py         # pipeline: sanity, export CSV/Spark
├── domain/                        # NOWE: czysta logika biznesowa (bez Spark/Kedro)
│   ├── __init__.py
│   ├── route_computation.py       # map_compute_provider_routes, map_compute_competitor_routes
│   ├── rac_evaluation.py          # map_rac (logika RAC)
│   ├── geometry.py                # convert_to_linestring, get_length, distance
│   └── metadata.py                # budowanie inspection_metadata
└── integration.py
```

#### 2.2 Kluczowy element: Kompozycja sub-pipeline'ów

Obecny pipeline to **jeden monolityczny** `kedro.pipeline.Pipeline` z 9 node'ami.
Propozycja: rozbicie na **3 sub-pipeline'y**, które można łączyć, rozszerzać i uruchamiać niezależnie.

```python
# pipeline.py — NOWA WERSJA

def create_pipeline(**kwargs) -> Pipeline:
    """Pełny pipeline inspekcji = pre + core + post.
    
    Dodanie nowego kroku (np. zapis do DB) wymaga:
    1. Utworzenia nowego sub-pipeline (np. db_export.py)
    2. Dodania go do sumy poniżej
    """
    return (
        create_pre_inspection_pipeline()    # walidacja, dedup
        + create_core_inspection_pipeline() # routing, RAC, FCD, merge
        + create_post_inspection_pipeline() # sanity, export
        # + create_db_export_pipeline()     # ← przyszłe rozszerzenie
    )
```

Kedro obsługuje operator `+` na Pipeline, który łączy node'y z zachowaniem
zależności danych (DAG). Dzięki temu:

- **Dodanie kroku "przed"** (np. walidacja danych wejściowych):
  dopisujemy node do `pre_inspection.py`, który produkuje output
  konsumowany przez `core`.

- **Dodanie kroku "po"** (np. zapis do bazy):
  tworzymy nowy plik z sub-pipeline'em konsumującym outputy `post`
  (np. `tbt_inspection_routes_sc`), dodajemy do sumy w `create_pipeline()`.

- **Uruchomienie części pipeline'u**:
  ```bash
  kedro run --pipeline=tbt_inspection_core   # tylko logika biznesowa
  kedro run --pipeline=tbt_inspection_post   # tylko eksport
  ```

#### 2.3 Szczegółowy podział sub-pipeline'ów

##### `pre_inspection` — walidacja i przygotowanie

```
Inputs: params:tbt_options, tbt_inspection_metadata_input_sql
Nodes:  check_duplicates (aktualnie zakomentowany — do odkomentowania)
Output: tbt_inspection_metadata_duplicates (bool)
```

Punkt rozszerzenia: tu dodajemy np. walidację parametrów, sprawdzenie 
dostępności API, pobranie danych z bazy.

##### `core_inspection` — logika biznesowa

```
Nodes:  get_provider_routes → reuse_static_routes → get_competitor_routes
        → get_rac_state → get_fcd_state → merge_inspection_data
```

To jest serce pipeline'u — nie powinno się go modyfikować przy dodawaniu
nowych kroków pre/post.

##### `post_inspection` — walidacja wyników i eksport

```
Nodes:  sanity_check → export_to_csv → export_to_spark → raise_sanity_error
```

Punkt rozszerzenia: tu dodajemy np. zapis do PostgreSQL, wysłanie 
notyfikacji, generowanie raportów.

#### 2.4 Przykład: dodanie zapisu do bazy danych (przyszłość)

```python
# nodes/db_export.py
def export_to_database(inspection_routes, critical_sections, metadata, db_credentials):
    engine = create_engine(db_credentials["connection_string"])
    inspection_routes.to_sql("inspection_routes", engine, if_exists="append")
    critical_sections.to_sql("inspection_critical_sections", engine, if_exists="append")
    metadata.to_sql("inspection_metadata", engine, if_exists="append")
    return True
```

```python
# pipelines/db_export.py
import kedro.pipeline
from ..nodes.db_export import export_to_database

def create_db_export_pipeline() -> kedro.pipeline.Pipeline:
    return kedro.pipeline.Pipeline([
        kedro.pipeline.node(
            func=export_to_database,
            inputs=[
                "tbt_inspection_routes_sc",
                "tbt_inspection_critical_sections_sc",
                "tbt_inspection_metadata_sc",
                "params:db_credentials",
            ],
            outputs="db_export_ok",
            name="tbt_export_to_database",
        ),
    ])
```

Dodanie do pipeline'u to **jedna linia** w `pipeline.py`:
```python
return (
    create_pre_inspection_pipeline()
    + create_core_inspection_pipeline()
    + create_post_inspection_pipeline()
    + create_db_export_pipeline()          # ← nowy krok
)
```

A rejestracja w `pipeline_registry.py` pozwala też na samodzielne uruchomienie:
```bash
kedro run --pipeline=tbt_inspection_db_export
```

#### 2.5 Rejestracja sub-pipeline'ów w pipeline_registry.py

```python
def register_pipelines() -> Dict[str, Pipeline]:
    return {
        # Pełny pipeline (domyślny)
        "tbt_inspection": inspection.create_pipeline(),
        
        # Sub-pipeline'y (do debugowania, testowania, lub selektywnego uruchamiania)
        "tbt_inspection_pre":  inspection.create_pre_inspection_pipeline(),
        "tbt_inspection_core": inspection.create_core_inspection_pipeline(),
        "tbt_inspection_post": inspection.create_post_inspection_pipeline(),
    }
```

#### 2.6 Korzyści

- ✅ **Minimalne ryzyko** — nie zmieniamy logiki, tylko organizację kodu
- ✅ **`nodes.py` z 1490 linii rozbity** na ~8 plików po ~150–200 linii
- ✅ **Warstwa `domain/` jest testowalna** bez Spark/Kedro
- ✅ **Kompatybilność wsteczna** — `kedro run --pipeline='tbt_inspection'` działa bez zmian
- ✅ **Łatwe dodawanie kroków** pre/post przez nowe sub-pipeline'y
- ✅ **Selektywne uruchamianie** — można uruchomić tylko `core` lub tylko `post`
- ✅ **Czytelna architektura** — nowy developer widzi 3 fazy, nie 9 node'ów w jednym pliku

#### 2.7 Wady

- ❌ Nie rozwiązuje problemu sprzężenia ze Spark w warstwie node'ów
- ❌ Nadal monolityczny projekt Kedro (ale z czystszą strukturą)

---

### Opcja B: Wydzielenie do osobnego pakietu Python (średni koszt)

Wydzielenie logiki biznesowej inspection do osobnego pakietu Python (`tbt-inspection-core`), który jest niezależny od Kedro.

*(Rozważyć jako Fazę 2, po wdrożeniu Opcji A)*

#### Zalety:
- ✅ Logika biznesowa w 100% niezależna od Kedro
- ✅ Testowalna bez Spark (unit testy na Pandas)
- ✅ Możliwość reuse w innych projektach/notebookach

#### Wady:
- ❌ Wymaga zarządzania dwoma pakietami
- ❌ Refaktoryzacja Spark → Pandas może być ryzykowna jeśli dane są duże
- ❌ Większy nakład pracy na początek

---

### Opcja C: Migracja z Kedro na standalone Python CLI (wysoki koszt, strategiczna)

**Rekomendacja: Ta opcja miałaby sens TYLKO jeśli Kedro aktywnie blokuje rozwój.**

---

## 3. Rekomendacja

### Podejście inkrementalne (Opcja A → B):

#### Faza 1 (1–2 dni): Refaktoryzacja wewnętrzna (Opcja A) ← IMPLEMENTOWANA
1. Rozbicie `nodes.py` na moduły tematyczne (w katalogu `nodes/`)
2. Wydzielenie czystej logiki biznesowej do `domain/`
3. Podział `pipeline.py` na sub-pipeline'y (`pre`, `core`, `post`)
4. Rejestracja sub-pipeline'ów w `pipeline_registry.py`
5. Wyczyszczenie martwego kodu
6. Dodanie type hints
7. Pokrycie `domain/` unit testami

#### Faza 2 (3–5 dni): Wydzielenie pakietu (Opcja B — opcjonalna)
1. Przeniesienie `domain/` do `tbt_inspection_core`
2. Typed configuration (`InspectionConfig`)
3. Thin Kedro wrappers
4. Rozważenie abstrakcji nad Spark

---

## 4. Quick wins — do wdrożenia natychmiast

### 4.1 Usunięcie martwego kodu z `pipeline_registry.py`
### 4.2 Rozbicie `nodes.py` (tabela w sekcji 2.1)
### 4.3 Typed config — zamiana surowego `dict` na `dataclass`
### 4.4 Cleanup parametrów — `error_classification_mode: False` vs `== "ML"`
