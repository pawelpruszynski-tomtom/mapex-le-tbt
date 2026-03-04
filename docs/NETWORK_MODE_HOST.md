# Network Mode: Host - Wyjaśnienie

## Co to jest network_mode: "host"?

Przy użyciu `network_mode: "host"` kontenery Docker **dzielą sieć z hostem**. Oznacza to:

✅ **Zalety:**
- Kontener używa **bezpośrednio portów hosta** (np. localhost:8000)
- **Łatwy dostęp** do usług na localhost (Redis, PostgreSQL)
- **Brak translacji NAT** - lepsza wydajność
- **Prostsza konfiguracja** - nie trzeba konfigurować Docker networks

⚠️ **Ograniczenia:**
- **Nie można mapować portów** (`ports:` jest ignorowane)
- Kontenery używają **tych samych portów co deklarują w aplikacji**
- Działa **tylko na Linux** (na macOS/Windows wymaga bridge network)

## Porty używane przez aplikację

Gdy używasz `network_mode: "host"`, serwisy są dostępne na **tych samych portach co w aplikacji**:

| Serwis | Port w aplikacji | Dostępny na hoście |
|--------|------------------|-------------------|
| tbt-api | 8000 | http://localhost:8000 |
| rq-dashboard | 9181 | http://localhost:9181 |
| Redis (external) | 6379 | localhost:6379 |
| PostgreSQL (external) | 5432 | localhost:5432 |

## Dlaczego usunięto mapowanie portów?

### PRZED (z ostrzeżeniami):
```yaml
tbt-api:
  ports:
    - "8000:8000"  # ⚠️ Ignorowane przy network_mode: host
  network_mode: "host"
```

**Docker wyświetlał ostrzeżenie:**
```
! Published ports are discarded when using host network mode
```

### PO (bez ostrzeżeń):
```yaml
tbt-api:
  # ports: nie potrzebne przy network_mode: host
  network_mode: "host"
```

**Aplikacja nadal dostępna na porcie 8000**, ale bezpośrednio z hosta (bez mapowania).

## Jak to działa?

### Network mode: host
```
┌─────────────────────────────────────┐
│         Host Network Stack          │
│                                      │
│  localhost:8000   ←  tbt-api        │
│  localhost:9181   ←  rq-dashboard   │
│  localhost:6379   ←  Redis (external)│
│  localhost:5432   ←  PostgreSQL (ext)│
│                                      │
│  Wszystko w tej samej przestrzeni   │
│  adresowej - jak natywne procesy    │
└─────────────────────────────────────┘
```

### Network mode: bridge (tradycyjny)
```
┌──────────────────────────────────────┐
│         Host Network Stack           │
│  localhost:8000  → NAT → Container   │
│  localhost:9181  → NAT → Container   │
└──────────────────────────────────────┘
         ↓ (translacja NAT)
┌──────────────────────────────────────┐
│      Docker Bridge Network           │
│  container1:8000 (tbt-api)           │
│  container2:9181 (rq-dashboard)      │
└──────────────────────────────────────┘
```

## Weryfikacja

Po uruchomieniu sprawdź dostępność:

```bash
# API
curl http://localhost:8000/health

# RQ Dashboard
curl http://localhost:9181

# Lub otwórz w przeglądarce:
# http://localhost:8000/docs
# http://localhost:9181
```

## Alternatywa: Bridge Network (dla macOS/Windows)

Jeśli `network_mode: "host"` nie działa (macOS/Windows), użyj bridge network:

```yaml
# docker-compose.yml
version: '3.8'

services:
  tbt-api:
    build: .
    ports:
      - "8000:8000"  # Mapowanie portów działa w bridge
    networks:
      - tbt-network
    # network_mode: "host"  # Usuń to
    environment:
      # Dla bridge network zmień localhost na nazwy serwisów lub IP hosta
      - REDIS_HOST=host.docker.internal  # macOS/Windows
      # - REDIS_HOST=172.17.0.1  # Linux (IP hosta z perspektywy Docker)
      - DB_HOST=host.docker.internal

  rq-dashboard:
    image: eoranged/rq-dashboard:latest
    ports:
      - "9181:9181"
    networks:
      - tbt-network
    environment:
      - RQ_DASHBOARD_REDIS_URL=redis://host.docker.internal:6379

networks:
  tbt-network:
    driver: bridge
```

## Kiedy używać host vs bridge?

### Użyj host gdy:
✅ Pracujesz na **Linux**  
✅ Redis i PostgreSQL są na **localhost**  
✅ Chcesz **prostą konfigurację**  
✅ Nie przeszkadza Ci brak izolacji sieciowej  

### Użyj bridge gdy:
✅ Pracujesz na **macOS lub Windows**  
✅ Potrzebujesz **izolacji sieciowej**  
✅ Chcesz **kontrolować mapowanie portów**  
✅ Redis/PostgreSQL są w **innych kontenerach**  

## Podsumowanie

✅ **Usunięto sekcje `ports:`** z docker-compose.yml  
✅ **Brak ostrzeżeń** podczas `docker-compose up`  
✅ **Serwisy nadal dostępne** na localhost:8000 i localhost:9181  
✅ **Łatwy dostęp** do Redis i PostgreSQL na localhost  

**Wszystko działa tak samo, ale bez ostrzeżeń!** 🎉

