#!/usr/bin/env bash
# =============================================================
# run_streamline.sh — collector en batches pequeños durante el día
#
# Uso:
#   bash pipeline/run_streamline.sh                  # batch=5, sleep=300s
#   bash pipeline/run_streamline.sh 3 180            # batch=3, sleep=3min
#   BATCH_SIZE=2 SLEEP_BETWEEN=120 bash pipeline/run_streamline.sh
#
# Lógica:
#   Divide los 30 tickers en batches y los procesa uno por uno,
#   acumulando en la partición de hoy en MinIO (append+dedup).
#   Al terminar todos los batches, lanza el ETL una sola vez.
# =============================================================
set -euo pipefail

BATCH_SIZE=${BATCH_SIZE:-${1:-5}}
SLEEP_BETWEEN=${SLEEP_BETWEEN:-${2:-5}}      # SEC no tiene límite diario, solo 10 req/s
TOTAL_TICKERS=30

log() { echo "[$(date '+%H:%M:%S')] [streamline] $1"; }

SEC_ONLY=${SEC_ONLY:-false}
OHLCV_ONLY=${OHLCV_ONLY:-false}

log "batch_size=$BATCH_SIZE | sleep=${SLEEP_BETWEEN}s | tickers=$TOTAL_TICKERS | sec_only=$SEC_ONLY | ohlcv_only=$OHLCV_ONLY"
log "Tiempo estimado: $((TOTAL_TICKERS / BATCH_SIZE * (SLEEP_BETWEEN + 120) / 60)) min aprox."

offset=0
batch_num=0

while [ "$offset" -lt "$TOTAL_TICKERS" ]; do
    batch_num=$((batch_num + 1))
    end=$((offset + BATCH_SIZE))
    [ "$end" -gt "$TOTAL_TICKERS" ] && end=$TOTAL_TICKERS
    actual_batch=$((end - offset))

    log "Batch $batch_num: tickers $offset–$((end - 1)) ($actual_batch tickers)"

    docker compose --profile pipeline run --rm \
        -e TICKER_OFFSET="$offset" \
        -e BATCH_SIZE="$BATCH_SIZE" \
        -e INITIAL_SLEEP=10 \
        -e SEC_ONLY="$SEC_ONLY" \
        -e OHLCV_ONLY="$OHLCV_ONLY" \
        collector

    offset=$((offset + BATCH_SIZE))

    if [ "$offset" -lt "$TOTAL_TICKERS" ]; then
        log "Batch $batch_num OK — esperando ${SLEEP_BETWEEN}s antes del siguiente..."
        sleep "$SLEEP_BETWEEN"
    fi
done

log "Todos los batches completados. Lanzando ETL..."
docker compose --profile pipeline run --rm etl

log "ETL completado. Actualizando capa OLAP (DuckDB)..."
make refresh-olap 2>&1 || true

log "=== Streamline completo ==="
