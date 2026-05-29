"""
profiler.py — análisis de calidad del source DataFrame.

Genera siempre:
  - JSON con stats básicas (null %, distinct, min/max)
  - Escribe a profiling.column_stats en PostgreSQL si hay run_id

Si ydata-profiling está instalado, genera además:
  - Reporte HTML interactivo en PROFILES_DIR/{date}/{name}.html

Los warnings retornados se loguean como WARNING en el ETL.
Un null_pct > 30% en columnas críticas lanza ValueError y aborta el package.
"""
import json
import logging
import os
from datetime import date
from pathlib import Path

import pandas as pd

log = logging.getLogger(__name__)

PROFILES_DIR = os.getenv("PROFILES_DIR", "/app/profiles")

CRITICAL_COLS = {
    "ohlcv":   ["ticker", "trade_date", "close", "adj_close"],
    "insider": ["reporter_cik", "transaction_date", "transaction_code"],
}


def generate_report(df: pd.DataFrame, name: str,
                    conn=None, run_id: int | None = None) -> list[str]:
    """
    Analiza df, guarda stats en JSON + BD (si hay conn/run_id) + HTML (si ydata disponible).
    Retorna lista de warnings. Lanza ValueError en columnas críticas con > 30% nulos.
    """
    warnings = []
    out_dir = Path(PROFILES_DIR) / date.today().isoformat()
    out_dir.mkdir(parents=True, exist_ok=True)

    stats = _basic_stats(df)

    # Persistir JSON local
    (out_dir / f"{name}_stats.json").write_text(json.dumps(stats, indent=2, default=str))
    log.info(f"[profiler] stats → {out_dir}/{name}_stats.json")

    # Persistir en profiling.column_stats si hay conexión DB
    if conn is not None and run_id is not None:
        _write_db_stats(conn, run_id, f"staging.raw_{name}", stats)

    # Generar warnings
    critical = CRITICAL_COLS.get(name, [])
    for col, info in stats.items():
        pct = info["null_pct"]
        if pct > 30 and col in critical:
            raise ValueError(f"[profiler:{name}] {col}: {pct:.1f}% nulos — abortando")
        if pct > 10:
            warnings.append(f"{col}: {pct:.1f}% nulos")
        elif pct > 1:
            warnings.append(f"{col}: {pct:.1f}% nulos (leve)")

    _try_html_report(df, name, out_dir)
    return warnings


def _basic_stats(df: pd.DataFrame) -> dict:
    result = {}
    total = len(df)
    for col in df.columns:
        null_count = int(df[col].isna().sum() + (df[col].astype(str) == "nan").sum())
        entry = {
            "total_rows":     total,
            "null_count":     null_count,
            "null_pct":       round(null_count / total * 100, 2) if total > 0 else 0,
            "distinct_count": int(df[col].nunique()),
        }
        if pd.api.types.is_numeric_dtype(df[col]):
            entry["min"] = float(df[col].min()) if not df[col].dropna().empty else None
            entry["max"] = float(df[col].max()) if not df[col].dropna().empty else None
        result[col] = entry
    return result


def _write_db_stats(conn, run_id: int, table_name: str, stats: dict) -> None:
    try:
        rows = [
            (run_id, table_name, col,
             info["total_rows"], info["null_count"], info["null_pct"],
             info["distinct_count"],
             str(info.get("min", "")) or None,
             str(info.get("max", "")) or None)
            for col, info in stats.items()
        ]
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO profiling.column_stats
                    (run_id, table_name, column_name, total_rows, null_count,
                     null_pct, distinct_count, min_val, max_val)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, rows)
        conn.commit()
        log.info(f"[profiler] column_stats → profiling.column_stats ({len(rows)} cols)")
    except Exception as e:
        log.warning(f"[profiler] No se pudieron guardar column_stats en BD: {e}")


def _upload_to_minio(local_path: Path, name: str) -> None:
    """Sube el reporte HTML a MinIO para acceso desde la UI del operador."""
    try:
        from minio import Minio
        endpoint   = os.getenv("MINIO_ENDPOINT",   "10.10.10.20:9000")
        access_key = os.getenv("MINIO_ACCESS_KEY",  "admin")
        secret_key = os.getenv("MINIO_SECRET_KEY",  "admin1234")
        bucket     = "profiling-reports"

        client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)

        object_name = f"{date.today().isoformat()}/{local_path.name}"
        client.fput_object(bucket, object_name, str(local_path), content_type="text/html")
        log.info(f"[profiler] MinIO  → s3://{bucket}/{object_name}")
    except Exception as e:
        log.debug(f"[profiler] MinIO upload omitido: {e}")


def _try_html_report(df: pd.DataFrame, name: str, out_dir: Path) -> None:
    try:
        from ydata_profiling import ProfileReport
        report = ProfileReport(
            df,
            title=f"Profile: {name} — {date.today().isoformat()}",
            minimal=True,
            progress_bar=False,
        )
        html_path = out_dir / f"{name}.html"
        report.to_file(str(html_path))
        log.info(f"[profiler] HTML  → {html_path}")
        _upload_to_minio(html_path, name)
    except ImportError:
        log.debug("[profiler] ydata-profiling no instalado — solo stats básicos")
    except Exception as e:
        log.warning(f"[profiler] Error generando HTML: {e}")
