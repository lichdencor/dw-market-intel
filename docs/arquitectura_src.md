---
title: "DW Analytics — Arquitectura Técnica"
subtitle: "Data Warehouse para análisis de mercado e insider trading\nSector tecnológico EE.UU. · Yahoo Finance + SEC EDGAR Form 4"
author: "Lucas Chavez · UB Base de Datos 2"
date: "Mayo 2026"
lang: es-AR
toc: true
toc-depth: 3
numbersections: true
---

# Objetivo

El sistema responde a una pregunta analítica concreta: **¿qué están haciendo los insiders del sector tech y qué relación tiene eso con el precio de las acciones?**

Se construyó un data warehouse que integra dos fuentes públicas — Yahoo Finance (precios OHLCV) y SEC EDGAR Form 4 (insider trading) — y los expone a través de una capa OLAP separada para análisis multidimensional.

Los tres principios de diseño que guían todas las decisiones:

1. **Separación de responsabilidades**: el DW (PostgreSQL) almacena y garantiza integridad; el motor OLAP (DuckDB) computa y escribe resultados; Metabase los visualiza.
2. **Un único entry point para el analista**: todo análisis pasa por Metabase. El analista (`bi_user`) solo puede ver el schema `cube` — acceso a facts crudas bloqueado estructuralmente por PostgreSQL.
3. **Observabilidad completa**: cada run del pipeline escribe en `profiling.*`, cada fila rechazada va a quarantine, cada cambio en dimensiones queda en historia, y Uptime Kuma monitorea servicios y heartbeat del pipeline.

---

# Qué capturamos

## Fuente 1 — Yahoo Finance (OHLCV)

30 tickers del sector tecnológico de EE.UU. (AAPL, MSFT, NVDA, GOOGL, META, etc.), un año de historial, granularidad diaria.

| Campo raw | Tipo | Descripción |
|---|---|---|
| `ticker` | Business key | Símbolo bursátil |
| `trade_date` | Temporal | Fecha de trading (días hábiles) |
| `open / high / low / close` | Medida | Precios OHLC del día |
| `adj_close` | Medida | Precio ajustado por splits y dividendos |
| `volume` | Medida | Acciones transaccionadas |

**Indicadores derivados** (calculados con window functions en el ETL, almacenados en `fact_price_daily`):

| Indicador | Fórmula | Uso analítico |
|---|---|---|
| `daily_return` | `adj_close − LAG(adj_close)` | Variación absoluta diaria |
| `pct_change` | `(adj_close / LAG) − 1` | Base de volatilidad y retorno |
| `rolling_30d_vol` | `STDDEV(pct_change) OVER 30d` | Volatilidad histórica |
| `sma_20 / 50 / 200` | `AVG(close) OVER N días` | Señales de tendencia |
| `bb_upper / bb_lower` | `SMA20 ± 2 × STDDEV(close, 20d)` | Bandas de Bollinger — sobrecompra/venta |
| `volume_ratio` | `volume / AVG(volume OVER 30d)` | Detección de spikes de volumen |
| `is_suspect` | `\|pct\_change\| > 15%` | Flag de dato anómalo |

## Fuente 2 — SEC EDGAR Form 4

Los insiders de empresas públicas tienen obligación legal de reportar sus transacciones en acciones dentro de las 2 jornadas hábiles mediante el formulario Form 4 (Filing Statement of Changes in Beneficial Ownership).

| Campo raw | Descripción |
|---|---|
| `accession_number` | Identificador único del filing en EDGAR |
| `reporter_cik` | CIK del insider (identificador único asignado por la SEC) |
| `reporter_name / title` | Nombre y cargo (CEO, CFO, Director, etc.) |
| `issuer_ticker` | Ticker de la empresa en la que opera |
| `transaction_date` | Fecha efectiva de la transacción |
| `transaction_code` | **P** = compra mercado abierto, **S** = venta, **A** = award, **M** = option exercise |
| `shares / price_per_share` | Cantidad y precio por acción |
| `shares_owned_after` | Posición del insider post-transacción |

**Grain del fact_insider_daily**: si un insider hace múltiples operaciones del mismo código el mismo día, se consolidan en 1 fila (`shares = SUM`, `price = promedio ponderado`, `shares_owned_after = MAX`).

---

# C4 Level 1 — Contexto del sistema

El sistema tiene **dos tipos de usuarios** con necesidades y accesos completamente distintos:

![Contexto del sistema — actores y fuentes externas](img/c4_01_context.png)

| Actor | Acceso | Qué hace |
|---|---|---|
| **Analista** | Metabase :3000 únicamente | Explora dashboards, consulta models y métricas, aplica filtros (ej: ticker = AAPL) |
| **Operador** | MinIO :9001 + Metabase (admin) + DuckDB CLI | Monitorea el estado del pipeline, verifica calidad de datos, diagnostica problemas |

> El analista **nunca** consulta PostgreSQL directamente. Todo análisis pasa por la capa OLAP.

---

# C4 Level 2 — Containers

El sistema se despliega en dos VMs KVM sobre la red `10.10.10.0/24`:

![Containers del sistema — separación DW (PostgreSQL) y OLAP (DuckDB)](img/c4_02_containers.png)

## VM 10.10.10.10 — Bare metal

PostgreSQL 17 corre sin Docker. Su único rol es el **Data Warehouse relacional**: recibir los upserts del ETL y garantizar integridad referencial. No contiene lógica de análisis multidimensional.

## VM 10.10.10.20 — Docker Compose

| Container | Puerto | Rol |
|---|---|---|
| **Collector** | — | Descarga OHLCV + Form 4, escribe CSVs a MinIO |
| **ETL** | — | Lee MinIO (incremental), profilea, carga PostgreSQL. Heartbeat a Uptime Kuma al terminar |
| **MinIO** | :9000/:9001 | Landing zone raw — CSVs inmutables por fecha |
| **DuckDB worker** | — | Motor OLAP — lee PostgreSQL via `postgres_scanner`, computa ROLLUP/CUBE/ASOF JOIN, escribe a PostgreSQL `cube` schema |
| **Metabase** | :3000 | BI — único entry point del Analista (`bi_user` solo ve `cube.*`) |
| **Nginx** | :8080 | Sirve reportes HTML de ydata-profiling (solo LAN) |
| **Uptime Kuma** | :3001 | Monitoreo de servicios + pipeline heartbeat (solo LAN) |

---

# C4 Level 3 — Components

## Pipeline (Collector + ETL)

![Components del pipeline — collector, ETL packages y flujo de datos](img/c4_03_pipeline.png)

### Collector

Descarga en batches configurables (`BATCH_SIZE=5`, `TICKER_OFFSET=N`) para evitar rate limiting. Cap de `MAX_FILINGS=500` por ticker en SEC (AAPL tiene 10.000+ filings por año sin cap). Append+dedup a MinIO en modo batch.

### ETL — Package pattern

```
ETLPackage.run()
  extract()  → lee CSV de MinIO (solo fechas/filings nuevos — incremental)
  profile()  → ydata-profiling HTML + column_stats → profiling.*
  stage()    → valida, quarantinea inválidos, TRUNCATE+COPY → staging.*
  load()     → upserts → dw.*
  heartbeat  → GET /api/push/<token> a Uptime Kuma
```

Advisory lock `pg_try_advisory_lock(20260529)` — aborta si otro ETL ya corre.

**ETL incremental**: en cada run, `OHLCVPackage` filtra las filas cuya `trade_date` ya existe en `fact_price_daily` por ticker. `InsiderPackage` omite los `accession_number` ya presentes en `fact_insider_daily`. Un run diario tarda segundos en vez de minutos. Se puede desactivar con `INCREMENTAL=false`.

**OHLCVPackage** calcula SMA 20/50/200 y Bollinger Bands como window functions en una tabla temporal (`_tmp_ohlcv`) antes del INSERT final, evitando dos lecturas sobre la fact.

**SCD Type 2** en `dim_ticker`: cuando cambia `sector`, `industry` o `market_cap_cat`, la versión anterior se archiva en `dim_ticker_history` con `valid_from`/`valid_to` antes de actualizar.

## Base de datos (PostgreSQL 17) — DW puro

![Schemas de la BD — staging, dw y profiling (sin OLAP)](img/c4_04_database.png)

### Separación de schemas

| Schema | Propósito | Quién escribe | Quién lee |
|---|---|---|---|
| `staging` | Zona de aterrizaje raw, UNLOGGED | ETL | ETL |
| `dw` | Star schema — hechos y dimensiones | ETL | DuckDB (via postgres_scanner) |
| `profiling` | Observabilidad del pipeline | ETL | Operador via Metabase |

El schema `dw` contiene **2 fact tables de negocio** y **1 fact derivada de optimización**:

| Tabla | Tipo | Grain |
|---|---|---|
| `fact_price_daily` | Fact de negocio | ticker × día × granularidad |
| `fact_insider_daily` | Fact de negocio | ticker × día × insider × tipo transacción |
| `fact_price_monthly_snapshot` | Fact periódica derivada | ticker × año × mes — optimización, no fuente primaria |

### Integridad referencial

8 foreign keys garantizan que ningún hecho puede existir sin su dimensión. Los `ON CONFLICT DO UPDATE` del ETL se resuelven usando la PK compuesta como conflicto target — por eso las PKs de facts son compuestas y no solo `BIGSERIAL`.

## Motor OLAP (DuckDB)

![Motor OLAP — DuckDB escribe a PostgreSQL cube schema → Metabase](img/c4_05_semantic.png)

### Por qué DuckDB y no PostgreSQL para OLAP

PostgreSQL es un motor OLTP (orientado a filas, transaccional). Para queries analíticas que escanean millones de filas y hacen múltiples groupings, es subóptimo. DuckDB es un motor **OLAP** (columnar, vectorizado, diseñado para analítica):

| Característica | PostgreSQL | DuckDB |
|---|---|---|
| Almacenamiento | Row-oriented | Columnar |
| Ejecución | Iterador tupla por tupla | Vectorizado (procesa batches) |
| GROUP BY complejos | Slow scan + sort | Bloom filter + hash aggregation |
| Window functions | Materializa en memoria | Streaming pipeline |
| `FIRST(expr ORDER BY col)` | No existe — workaround DISTINCT ON | Aggregate nativo |
| `ASOF JOIN` | No disponible | Nativo — join al precio más cercano en fecha |

### Flujo DuckDB → PostgreSQL → Metabase

DuckDB lee el DW via `postgres_scanner` (federación read-only), computa las tablas analíticas, y las escribe a PostgreSQL `schema cube` via psycopg2. Metabase conecta a PostgreSQL y lee desde `cube.*`.

```
PostgreSQL dw.* (DW)
    ↓ postgres_scanner (read-only)
DuckDB (ROLLUP, CUBE, ASOF JOIN, FIRST ORDER BY)
    ↓ psycopg2 COPY (write results)
PostgreSQL cube.* (OLAP results)
    ↓ JDBC
Metabase (bi_user — solo lee cube.*)
```

Este diseño separa el motor de cómputo (DuckDB) del almacenamiento de resultados (PostgreSQL), permitiendo que Metabase use su conector PostgreSQL nativo sin drivers adicionales.

---

# C4 Level 4 — Modelo de datos

## Star Schema (ER diagram)

![Star Schema — ER diagram con tipos, PKs, FKs y UKs](img/03_star_schema.png)

**Dimensiones conformadas**: `dim_ticker` y `dim_date` son referenciadas por ambas facts con el mismo significado — esto habilita JOINs cruzados directos entre `fact_price_daily` y `fact_insider_daily` para cruzar el precio del día con la actividad insider del mismo día y ticker.

## Flujos de transformación

### Pipeline OHLCV — window functions encadenadas

El ETL materializa los indicadores técnicos en una sola pasada sobre los datos, sin volver a leer la tabla:

```
staging.raw_ohlcv
    → base CTE:   casteo tipos + JOIN dim_ticker
    → lags CTE:   LAG(adj_close) → daily_return, pct_change
    → vol CTE:    STDDEV rolling 30d → rolling_30d_vol
                  AVG rolling 30d → volume_ratio
    → ind CTE:    AVG 20/50/200d → sma_20/50/200
                  SMA20 ± 2σ → bb_upper, bb_lower
    → INSERT INTO fact_price_daily (ON CONFLICT DO UPDATE)
```

### Pipeline insider — agregación diaria

```
staging.raw_insider
    → clean CTE:  filtro regex, casteo numérico
    → agg CTE:    GROUP BY (ticker, insider, código, fecha)
                  SUM(shares), precio ponderado, MAX(shares_owned_after)
    → INSERT INTO fact_insider_daily (ON CONFLICT DO UPDATE)
```

---

# Stack tecnológico

| Capa | Tecnología | VM | Nota crítica |
|---|---|---|---|
| Extracción OHLCV | Python + yfinance ≥1.4.1 | 10.10.10.20 | **NO pinchar a 0.2.54** — genera rate limit falsos positivos. SEC_SLEEP=0.125s (8 req/s) |
| Extracción Form 4 | Python + requests | 10.10.10.20 | URL Archives: `www.sec.gov` (NO `data.sec.gov`). Filtro `/xsl` en XML. MAX_FILINGS=500 |
| Landing | MinIO (S3-compatible) | 10.10.10.20 | Credenciales en `.env` |
| Profiling | ydata-profiling 4.9 | 10.10.10.20 | Requiere `setuptools==69.5.1` como capa Docker separada. HTML → nginx :8080 + MinIO |
| Staging | PostgreSQL UNLOGGED TEXT | 10.10.10.10 | TEXT para no castear datos sucios. Quarantine para filas inválidas |
| Data Warehouse | PostgreSQL 17 — DW puro | 10.10.10.10 | BD: `dw_analytics`. PKs, FKs, CHECKs, SCD2, RLS, audit trail, pg_stat_statements |
| Motor OLAP | DuckDB 1.1 | 10.10.10.20 | Columnar + vectorizado. Lee PG via `postgres_scanner`. Escribe resultados a `cube.*` |
| Resultados OLAP | PostgreSQL schema `cube` | 10.10.10.10 | Computado por DuckDB, almacenado en PG para acceso desde Metabase sin driver especial |
| ETL incremental | OHLCVPackage + InsiderPackage | 10.10.10.20 | Filtra fechas/accession_numbers ya cargados. `INCREMENTAL=false` para full reload |
| Visualización | Metabase v0.59 | 10.10.10.20 | `bi_user` → solo `cube.*` (Analista). `dw_user` → `dw + profiling` (Operador) |
| Monitoreo | Uptime Kuma | 10.10.10.20 | Servicios HTTP/TCP + pipeline heartbeat push. Solo LAN :3001 |
| Scheduler | pg_cron | 10.10.10.10 | `shared_preload_libraries` — editar `postgresql.conf` directo, NO `ALTER SYSTEM` |
| Orquestación | Bash + Makefile | 10.10.10.20 | `make streamline` para pipeline completo. `make refresh-olap` para OLAP |
| Exposición pública | Cloudflare Tunnel + Anubis | 10.10.10.20 | HTTPS gratuito + proof-of-work anti-scraper. URLs temporales con `make expose` |

---

# Decisiones de diseño

## PostgreSQL como DW puro — sin OLAP

La versión anterior tenía un schema `cube` en PostgreSQL con materialized views ROLLUP/CUBE. Esto viola la separación DW / OLAP: PostgreSQL es un motor OLTP (orientado a filas), no está diseñado para scans analíticos masivos. Las MVs eran un workaround razonable pero no la solución correcta.

**Decisión**: mover toda la lógica multidimensional a DuckDB. PostgreSQL queda exclusivamente como capa de almacenamiento relacional con garantías de integridad.

## Staging TEXT en todo

Las APIs externas devuelven strings con valores sucios (`"nan"`, `"None"`, `"0.0"`). Castear en staging introduce errores silenciosos. En staging todo es TEXT; el ETL castea con guards de regex en las CTEs del INSERT final.

## PK compuesta en las facts

El `ON CONFLICT (ticker_id, date_id, ...)` del ETL necesita la PK compuesta para upserts idempotentes. Garantiza unicidad de negocio a nivel de BD. `fact_id BIGSERIAL` tiene `UNIQUE` separado para referencias externas.

## Un solo entry point para el analista (Metabase + bi_user)

El analista nunca consulta PostgreSQL directamente. Se conecta como `bi_user` — un rol PostgreSQL que solo tiene `SELECT` en el schema `cube`. Intentar `SELECT * FROM dw.fact_price_daily` devuelve `ERROR: permiso denegado al esquema dw`. El enforcement es estructural, no convencional.

## SCD Type 2 ligero en dim_ticker

En lugar de la implementación full SCD2 con surrogate keys y FK migration en facts, se optó por una tabla de historial separada (`dim_ticker_history`). Los hechos siguen referenciando la versión actual de la dimensión. El historial queda consultable para análisis de "¿en qué sector estaba este ticker en 2025?".

---

# Estado de implementación

| Componente | Estado |
|---|---|
| PostgreSQL DW (staging + dw + profiling) | ✅ |
| Pipeline Collector + ETL con packages | ✅ |
| ETL incremental (skip fechas/filings ya cargados) | ✅ |
| Observabilidad (profiling schema, quarantine, ydata-profiling HTML) | ✅ |
| SCD Type 2 (dim_ticker_history) | ✅ |
| Indicadores técnicos (SMA 20/50/200, Bollinger, volume_ratio, is_suspect) | ✅ |
| DuckDB como motor OLAP (ROLLUP, CUBE, ASOF JOIN) | ✅ |
| Control de acceso: bi_user solo ve cube.*, dw_user ve dw + profiling | ✅ |
| Metabase: dashboard, models, metrics, 2 conexiones por rol | ✅ |
| Uptime Kuma + pipeline heartbeat | ✅ |
| Exposición pública: Cloudflare Tunnel + Anubis PoW | ✅ |
| Repo GitHub público: lichdencor/dw-market-intel | ✅ |

---

# Guía de operaciones

## Levantar el entorno

```bash
# 10.10.10.20 — stack Docker
cd ~/monolithic
make up          # MinIO + Metabase + Uptime Kuma
```

## Correr el pipeline

```bash
# Pipeline completo (30 tickers, 90 días, cap 500 filings/ticker)
# Incremental por default — solo procesa datos nuevos
tmux new-session -d -s dw "BATCH_SIZE=5 SLEEP_BETWEEN=5 PERIOD_DAYS=90 bash pipeline/run_streamline.sh 2>&1 | tee /tmp/streamline.log"
tmux attach -t dw

make etl           # ETL una vez (partition = hoy, incremental)
make etl-date DATE=2026-03-26   # ETL sobre partición histórica
make refresh-olap  # DuckDB computa → escribe a PostgreSQL cube
```

## Verificación

```bash
make counts   # row counts de todas las tablas del DW
make check    # integridad referencial
```

## Accesos

| Servicio | URL local | Rol | Credenciales |
|---|---|---|---|
| **Metabase** | http://10.10.10.20:3000 | Analista + Operador | ver `.env` (`MB_ADMIN_EMAIL`) |
| **MinIO UI** | http://10.10.10.20:9001 | Operador | ver `.env` (`MINIO_ROOT_USER`) |
| **Profiler UI** | http://10.10.10.20:8080 | Operador | — |
| **Uptime Kuma** | http://10.10.10.20:3001 | Operador | `operador` / ver `.env` |
| PostgreSQL | 10.10.10.10:5432 | ETL / DBA | ver `.env` (`DB_USER`) |

**URLs públicas temporales** (cambian con cada `make expose`):

```bash
make expose    # genera 3 URLs HTTPS via Cloudflare Tunnel + Anubis
make unexpose  # cierra los tunnels
```

---

*Documento generado desde `docs/arquitectura_src.md` — Mayo 2026*
