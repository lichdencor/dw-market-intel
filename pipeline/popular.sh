#!/usr/bin/env bash
set -euo pipefail

PARTITION_DATE=$(date +%Y-%m-%d)

log() { echo -e "[popular] $1"; }

log "=== Collector ==="

log "Reconstruyendo imagen collector..."
docker build -t collector:latest ./pipeline/collector/

log "Eliminando contenedor anterior si existe..."
docker rm -f collector 2>/dev/null || true

log "Lanzando collector..."
docker run --name collector \
  -e "MINIO_ENDPOINT=${MINIO_ENDPOINT:-localhost:9000}" \
  -e "MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-minioadmin}" \
  -e "MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-minioadmin}" \
  -e "MINIO_BUCKET=${MINIO_BUCKET:-raw-ohlcv}" \
  -e "PARTITION_DATE=$PARTITION_DATE" \
  -e "PERIOD_DAYS=${PERIOD_DAYS:-365}" \
  --network host \
  collector:latest

log "Collector completado"
