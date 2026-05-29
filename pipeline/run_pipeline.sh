#!/usr/bin/env bash
set -euo pipefail

log() { echo -e "[pipeline] $1"; }

log "=== Run Pipeline ==="

log "1. Ejecutando popular.sh (collector)"
bash pipeline/popular.sh

log "2. Ejecutando etl.sh (ETL)"
bash pipeline/etl.sh

log "Pipeline completo"
