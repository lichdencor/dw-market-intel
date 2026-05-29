"""
ETL — MinIO → staging → dw (PostgreSQL monolítico)
====================================================
Orquestador: instancia y corre los packages por fuente.

Variables de entorno:
  MINIO_ENDPOINT    default: 10.10.10.20:9000
  MINIO_ACCESS_KEY  default: admin
  MINIO_SECRET_KEY  default: admin1234
  MINIO_BUCKET      default: raw-ohlcv
  DB_HOST           default: 10.10.10.10
  DB_PORT           default: 5432
  DB_NAME           default: dw_analytics
  DB_USER           default: etl_user
  DB_PASSWORD       default: etl123
  PARTITION_DATE    default: fecha de hoy (YYYY-MM-DD)
"""

import json
import logging
import os
from datetime import date, datetime

import psycopg2

from packages import OHLCVPackage, InsiderPackage


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps({
            "ts":     datetime.fromtimestamp(record.created).isoformat(timespec="seconds"),
            "level":  record.levelname,
            "logger": record.name,
            "msg":    record.getMessage(),
        }, ensure_ascii=False)

# =============================================================
# CONFIGURACIÓN
# =============================================================

MINIO_CONFIG = {
    "endpoint":   os.getenv("MINIO_ENDPOINT",   "10.10.10.20:9000"),
    "access_key": os.getenv("MINIO_ACCESS_KEY",  "admin"),
    "secret_key": os.getenv("MINIO_SECRET_KEY",  "admin1234"),
    "secure":     False,
    "bucket":     os.getenv("MINIO_BUCKET",      "raw-ohlcv"),
}

DB_CONFIG = {
    "host":     os.getenv("DB_HOST",     "10.10.10.10"),
    "port":     int(os.getenv("DB_PORT", "5432")),
    "dbname":   os.getenv("DB_NAME",     "dw_analytics"),
    "user":     os.getenv("DB_USER",     "etl_user"),
    "password": os.getenv("DB_PASSWORD", "etl123"),
}

PARTITION_DATE = os.getenv("PARTITION_DATE", date.today().isoformat())

_handler = logging.StreamHandler()
_handler.setFormatter(_JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[_handler], force=True)
log = logging.getLogger("etl")


# =============================================================
# MAIN
# =============================================================

_ETL_LOCK_KEY = 20260529   # clave arbitraria única para este ETL


def main():
    log.info("=" * 55)
    log.info(f"ETL — MinIO ({PARTITION_DATE}) → PostgreSQL")
    log.info("=" * 55)

    conn = psycopg2.connect(**DB_CONFIG)

    # Advisory lock — evita dos runs concurrentes
    with conn.cursor() as _cur:
        _cur.execute("SELECT pg_try_advisory_lock(%s)", (_ETL_LOCK_KEY,))
        locked = _cur.fetchone()[0]
    if not locked:
        log.error("Otro ETL ya está corriendo (advisory lock activo). Abortando.")
        conn.close()
        raise SystemExit(1)
    log.info("Advisory lock adquirido")

    try:
        packages = [
            OHLCVPackage(conn, MINIO_CONFIG, PARTITION_DATE),
            InsiderPackage(conn, MINIO_CONFIG, PARTITION_DATE),
        ]

        results = []
        for pkg in packages:
            try:
                result = pkg.run()
                results.append(result)
            except Exception as e:
                log.error(f"{pkg.__class__.__name__} falló: {e}")
                raise
    finally:
        # Liberar advisory lock antes de cerrar
        try:
            with conn.cursor() as _cur:
                _cur.execute("SELECT pg_advisory_unlock(%s)", (_ETL_LOCK_KEY,))
        except Exception:
            pass
        conn.close()

    total = sum(r.rows_loaded for r in results)
    log.info("=" * 55)
    log.info(f"ETL completado — {total:,} filas procesadas en {len(results)} packages")
    log.info("=" * 55)


if __name__ == "__main__":
    main()
