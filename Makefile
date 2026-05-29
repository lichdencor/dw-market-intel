# =============================================================
# Makefile — monolithic DW | VM 10.10.10.20
# Uso: make <target>
# =============================================================

-include .env
export

COMPOSE         := docker compose
PIPELINE        := docker compose --profile pipeline
DB_CONTAINER    := postgres:15
PSQL            := docker run --rm -e PGPASSWORD=$(DW_PASSWORD) $(DB_CONTAINER) \
                       psql -h $(DB_HOST) -U $(DW_USER) -d $(DB_NAME)
PSQL_ETL        := docker run --rm -e PGPASSWORD=$(DB_PASSWORD) $(DB_CONTAINER) \
                       psql -h $(DB_HOST) -U $(DB_USER) -d $(DB_NAME)

.DEFAULT_GOAL := help

# -------------------------------------------------------------
# Servicios
# -------------------------------------------------------------

.PHONY: up
up:                           ## Levanta MinIO + Metabase + Uptime Kuma
	$(COMPOSE) up -d minio metabase uptime-kuma

.PHONY: expose
expose:                       ## Expone Metabase + Profiler + MinIO via Cloudflare Tunnel (Uptime Kuma es solo interno)
	$(COMPOSE) --profile expose up -d cf-metabase cf-profiler cf-minio
	@echo "Esperando URLs (10s)..."
	@sleep 10
	@echo "=== URLs públicas ==="
	@docker logs cf-metabase 2>&1 | grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' | tail -1 | xargs -I{} echo "  Metabase  → {}"
	@docker logs cf-profiler 2>&1 | grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' | tail -1 | xargs -I{} echo "  Profiler  → {}"
	@docker logs cf-minio    2>&1 | grep -o 'https://[a-z0-9-]*\.trycloudflare\.com' | tail -1 | xargs -I{} echo "  MinIO     → {}"
	@echo "  Uptime Kuma → http://10.10.10.20:3001  (solo LAN)"

.PHONY: unexpose
unexpose:                     ## Cierra los túneles Cloudflare
	$(COMPOSE) --profile expose stop cf-metabase cf-profiler cf-minio
	$(COMPOSE) --profile expose rm -f cf-metabase cf-profiler cf-minio

.PHONY: down
down:                         ## Detiene todos los contenedores
	$(COMPOSE) down

.PHONY: ps
ps:                           ## Estado de contenedores
	$(COMPOSE) ps

# -------------------------------------------------------------
# Pipeline
# -------------------------------------------------------------

.PHONY: collector
collector:                    ## Corre el collector (todos los tickers, modo full)
	$(PIPELINE) run --rm collector

.PHONY: etl
etl:                          ## Corre el ETL contra la partición de hoy
	$(PIPELINE) run --rm etl

.PHONY: etl-date
etl-date:                     ## Corre ETL contra una fecha: make etl-date DATE=2026-03-26
	$(PIPELINE) run --rm -e PARTITION_DATE=$(DATE) etl

.PHONY: streamline
streamline:                   ## Collector en batches pequeños + ETL al final
	bash pipeline/run_streamline.sh

.PHONY: streamline-fast
streamline-fast:              ## Streamline con batch=3 y sleep=120s (más rápido, más riesgo)
	BATCH_SIZE=3 SLEEP_BETWEEN=120 bash pipeline/run_streamline.sh

# -------------------------------------------------------------
# Base de datos
# -------------------------------------------------------------

.PHONY: check
check:                        ## Verifica integridad referencial del DW
	$(PSQL) -c "SELECT * FROM dw.check_referential_integrity();"

.PHONY: counts
counts:                       ## Row counts de todas las tablas
	$(PSQL) -c "\
	SELECT 'dim_ticker'          AS tabla, COUNT(*) FROM dw.dim_ticker    UNION ALL \
	SELECT 'dim_date',                     COUNT(*) FROM dw.dim_date      UNION ALL \
	SELECT 'dim_insider',                  COUNT(*) FROM dw.dim_insider   UNION ALL \
	SELECT 'fact_price_daily',             COUNT(*) FROM dw.fact_price_daily UNION ALL \
	SELECT 'fact_insider_daily',           COUNT(*) FROM dw.fact_insider_daily \
	ORDER BY tabla;"

.PHONY: refresh
refresh: refresh-olap         ## Alias para refresh-olap

.PHONY: refresh-olap
refresh-olap:                 ## DuckDB computa OLAP → escribe a PostgreSQL cube schema
	@echo "Computando tablas OLAP en DuckDB → PostgreSQL cube..."
	$(PIPELINE) run --rm duckdb-setup
	@echo "DuckDB → PostgreSQL cube: actualizado. Metabase ya lo puede consultar."

# -------------------------------------------------------------
# Logs y utilidades
# -------------------------------------------------------------

.PHONY: logs
logs:                         ## Logs del ETL (follow)
	$(COMPOSE) logs -f etl

.PHONY: minio-ls
minio-ls:                     ## Lista las particiones en MinIO
	docker exec minio mc ls local/raw-ohlcv/ 2>/dev/null || \
	    docker exec minio mc alias set local http://localhost:9000 \
	        $(MINIO_ROOT_USER) $(MINIO_ROOT_PASSWORD) 2>/dev/null && \
	    docker exec minio mc ls local/raw-ohlcv/

.PHONY: build
build:                        ## Reconstruye las imágenes del pipeline
	$(COMPOSE) build collector etl

.PHONY: help
help:                         ## Muestra este mensaje
	@cat $(MAKEFILE_LIST) | grep -E '^[a-zA-Z_-]+:.*?## .*$$' | \
	    awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'
