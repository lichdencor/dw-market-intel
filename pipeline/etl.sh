#!/usr/bin/env bash
set -euo pipefail

PARTITION_DATE=$(date +%Y-%m-%d)

log() { echo -e "[etl] $1"; }

log "Reconstruyendo imagen etl..."
docker build -t etl:latest ./pipeline/etl/

log "Eliminando contenedor anterior si existe..."
docker rm -f etl 2>/dev/null || true

log "Ejecutando ETL..."
docker run --name etl \
  -e "MINIO_ENDPOINT=${MINIO_ENDPOINT:-localhost:9000}" \
  -e "MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-minioadmin}" \
  -e "MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-minioadmin}" \
  -e "MINIO_BUCKET=${MINIO_BUCKET:-raw-ohlcv}" \
  -e "PARTITION_DATE=$PARTITION_DATE" \
  -e "DB_HOST=${DB_HOST:-localhost}" \
  -e "DB_PORT=${DB_PORT:-5432}" \
  -e "DB_NAME=${DB_NAME:-datawarehouse}" \
  -e "DB_USER=${DB_USER:-postgres}" \
  -e "DB_PASSWORD=${DB_PASSWORD:-postgres}" \
  --network host \
  etl:latest

log "ETL completado"
