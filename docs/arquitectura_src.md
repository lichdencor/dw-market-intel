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

1. **Separación de responsabilidades**: el DW (PostgreSQL) almacena y garantiza integridad; el motor OLAP (DuckDB) analiza; la capa semántica (Cube.js) modela; la presentación (Metabase) visualiza.
2. **Un único entry point para el analista**: todo análisis pasa por Metabase. El analista nunca consulta PostgreSQL directamente.
3. **Observabilidad completa**: cada run del pipeline escribe en `profiling.*`, cada fila rechazada va a quarantine, cada cambio en dimensiones queda en historia.

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
| **ETL** | — | Lee MinIO, profilea, carga PostgreSQL |
| **MinIO** | :9000/:9001 | Landing zone raw — CSVs inmutables por fecha |
| **DuckDB** | :1294 | Motor OLAP — lee PostgreSQL via `postgres_scanner`, define cubos analíticos |
| **Cube.js** | :4000/:15432 | Semantic layer sobre DuckDB — models YAML, pre-aggregations |
| **Metabase** | :3000 | BI — único entry point del Analista |

---

# C4 Level 3 — Components

## Pipeline (Collector + ETL)

![Components del pipeline — collector, ETL packages y flujo de datos](img/c4_03_pipeline.png)

### Collector

Descarga en batches configurables (`BATCH_SIZE=5`, `TICKER_OFFSET=N`) para evitar rate limiting. Cap de `MAX_FILINGS=500` por ticker en SEC (AAPL tiene 10.000+ filings por año sin cap). Append+dedup a MinIO en modo batch.

### ETL — Package pattern

```
ETLPackage.run()
  extract()  → lee CSV de MinIO
  profile()  → ydata-profiling HTML + column_stats → profiling.*
  stage()    → valida, quarantinea inválidos, TRUNCATE+COPY → staging.*
  load()     → upserts → dw.*
```

Advisory lock `pg_try_advisory_lock(20260529)` — aborta si otro ETL ya corre.

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

## Motor OLAP (DuckDB) y Semantic Layer (Cube.js)

![Motor OLAP — DuckDB + Cube.js como capas separadas sobre PostgreSQL](img/c4_05_semantic.png)

### Por qué DuckDB y no PostgreSQL para OLAP

PostgreSQL es un motor OLTP (orientado a filas, transaccional). Para queries analíticas que escanean millones de filas y hacen múltiples groupings, es subóptimo. DuckDB es un motor **OLAP** (columnar, vectorizado, diseñado para analítica):

| Característica | PostgreSQL | DuckDB |
|---|---|---|
| Almacenamiento | Row-oriented | Columnar |
| Ejecución | Iterador tupla por tupla | Vectorizado (procesa batches) |
| GROUP BY complejos | Slow scan + sort | Bloom filter + hash aggregation |
| Window functions | Materializa en memoria | Streaming pipeline |
| `FIRST(expr ORDER BY col)` | No existe — workaround DISTINCT ON | Aggregate nativo |

DuckDB lee PostgreSQL via la extensión `postgres_scanner` (federación read-only sin ETL extra). Define vistas analíticas equivalentes a los cubos multidimensionales.

### Cube.js como semantic layer

Cube.js se posiciona sobre DuckDB y expone el modelo de datos como medidas y dimensiones consultables. Los analistas no escriben SQL — consultan `Price.ytd_return_pct` agrupado por `Ticker.sector` y Cube.js genera el SQL óptimo sobre DuckDB.

**Nota sobre el SQL API de Cube.js**: el SQL API (`:15432`) no resuelve dimensiones de cubos joineados en queries cross-cube. Para eso se usa el REST API (`:4000`). Metabase conecta via SQL API para queries simples y via REST para queries complejas.

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
| Extracción OHLCV | Python + yfinance ≥1.4.1 | 10.10.10.20 | **NO pinchar a 0.2.54** — genera rate limit falsos positivos |
| Extracción Form 4 | Python + requests | 10.10.10.20 | URL Archives: `www.sec.gov` (NO `data.sec.gov`). Filtro `/xsl` en XML |
| Landing | MinIO (S3-compatible) | 10.10.10.20 | Credenciales en `.env` |
| Profiling | ydata-profiling 4.9 | 10.10.10.20 | Requiere `setuptools==69.5.1` como capa Docker separada (Python 3.12-slim) |
| Staging | PostgreSQL UNLOGGED TEXT | 10.10.10.10 | TEXT para no castear datos sucios. Quarantine para filas inválidas |
| Data Warehouse | PostgreSQL 17 — DW puro | 10.10.10.10 | BD: `dw_analytics`. PKs, FKs, CHECKs, SCD2, RLS, audit. SIN OLAP |
| Motor OLAP | DuckDB | 10.10.10.20 | Columnar + vectorizado. Lee PG via `postgres_scanner`. **Pendiente implementar** |
| Semantic layer | Cube.js 1.6 | 10.10.10.20 | REST :4000 + SQL API :15432. Apuntará a DuckDB |
| Visualización | Metabase v0.59 | 10.10.10.20 | Único entry point del Analista. Metrics API: `POST /api/card` con `type:"metric"` |
| Scheduler | pg_cron | 10.10.10.10 | `shared_preload_libraries` — editar `postgresql.conf` directo, NO `ALTER SYSTEM` |
| Orquestación | Bash + Makefile + tmux | 10.10.10.20 | `make streamline` para pipeline completo |

---

# Decisiones de diseño

## PostgreSQL como DW puro — sin OLAP

La versión anterior tenía un schema `cube` en PostgreSQL con materialized views ROLLUP/CUBE. Esto viola la separación DW / OLAP: PostgreSQL es un motor OLTP (orientado a filas), no está diseñado para scans analíticos masivos. Las MVs eran un workaround razonable pero no la solución correcta.

**Decisión**: mover toda la lógica multidimensional a DuckDB. PostgreSQL queda exclusivamente como capa de almacenamiento relacional con garantías de integridad.

## Staging TEXT en todo

Las APIs externas devuelven strings con valores sucios (`"nan"`, `"None"`, `"0.0"`). Castear en staging introduce errores silenciosos. En staging todo es TEXT; el ETL castea con guards de regex en las CTEs del INSERT final.

## PK compuesta en las facts

El `ON CONFLICT (ticker_id, date_id, ...)` del ETL necesita la PK compuesta para upserts idempotentes. Garantiza unicidad de negocio a nivel de BD. `fact_id BIGSERIAL` tiene `UNIQUE` separado para referencias externas.

## Un solo entry point para el analista (Metabase)

El analista nunca consulta PostgreSQL directamente. Todos los análisis pasan por Metabase → Cube.js → DuckDB → PostgreSQL. Esto garantiza que las queries siempre pasen por la capa semántica (measures, dimensions, filtros pre-definidos) y que el DW esté protegido de queries ad-hoc sin control.

## SCD Type 2 ligero en dim_ticker

En lugar de la implementación full SCD2 con surrogate keys y FK migration en facts, se optó por una tabla de historial separada (`dim_ticker_history`). Los hechos siguen referenciando la versión actual de la dimensión. El historial queda consultable para análisis de "¿en qué sector estaba este ticker en 2025?".

---

# Estado de implementación

| Componente | Estado |
|---|---|
| PostgreSQL DW (staging + dw + profiling) | ✅ Implementado |
| Pipeline Collector + ETL con packages | ✅ Implementado |
| Observabilidad (profiling schema, quarantine) | ✅ Implementado |
| SCD Type 2 (dim_ticker_history) | ✅ Implementado |
| Indicadores técnicos (SMA, Bollinger, volume_ratio) | ✅ Implementado |
| Cube.js semantic layer (sobre PostgreSQL — temporal) | ✅ Implementado (transitorio) |
| Metabase: dashboard, models, metrics | ✅ Implementado |
| **DuckDB como motor OLAP** | ⏳ Pendiente |
| **Migrar schema cube → DuckDB** | ⏳ Pendiente (depende de DuckDB) |
| **Apuntar Cube.js a DuckDB** | ⏳ Pendiente |

---

# Guía de operaciones

## Levantar el entorno

```bash
# 10.10.10.20 — stack Docker
cd ~/monolithic
make up     # MinIO + Metabase
make cube   # Cube.js + CubeStore
```

## Correr el pipeline

```bash
# Pipeline en batches (30 tickers, 90 días, cap 200 filings/ticker)
tmux new-session -d -s dw "BATCH_SIZE=5 SLEEP_BETWEEN=60 PERIOD_DAYS=90 MAX_FILINGS=200 bash pipeline/run_streamline.sh 2>&1 | tee /tmp/streamline.log"
tmux attach -t dw

make etl-date DATE=2026-03-26   # ETL sobre partición histórica
```

## Verificación

```bash
make counts   # row counts de todas las tablas del DW
make check    # integridad referencial
```

## Accesos

| Servicio | URL | Credenciales |
|---|---|---|
| Metabase (Analista) | http://10.10.10.20:3000 | `MB_ADMIN_EMAIL` / `MB_ADMIN_PASSWORD` (ver `.env`) |
| MinIO (Operador) | http://10.10.10.20:9001 | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` (ver `.env`) |
| Cube.js Playground (Operador) | http://10.10.10.20:4000 | — |

---

*Documento generado desde `docs/arquitectura_src.md` — Mayo 2026*
