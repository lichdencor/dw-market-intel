"""
Utilidades compartidas entre packages: MinIO client, dim_date, helpers.
"""
import io
import logging
from datetime import datetime, timedelta

import pandas as pd
from minio import Minio

log = logging.getLogger(__name__)

MONTHS = ["January","February","March","April","May","June",
          "July","August","September","October","November","December"]
DAYS   = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]


def get_minio_client(config: dict) -> Minio:
    client = Minio(
        config["endpoint"],
        access_key=config["access_key"],
        secret_key=config["secret_key"],
        secure=config.get("secure", False),
    )
    bucket = config["bucket"]
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        log.info(f"Bucket '{bucket}' creado")
    return client


def read_csv(client: Minio, bucket: str, partition: str, filename: str) -> pd.DataFrame:
    object_name = f"{partition}/{filename}"
    log.info(f"Leyendo s3://{bucket}/{object_name}")
    response = client.get_object(bucket, object_name)
    df = pd.read_csv(io.BytesIO(response.read()))
    log.info(f"  {len(df):,} filas")
    return df


def populate_dim_date(conn, date_strings: list) -> None:
    # Normalizar a string primero para manejar tanto str como datetime.date de PG
    normalized = [str(d) for d in date_strings if d is not None]
    dates = pd.to_datetime(normalized, errors="coerce").dropna()
    unique = sorted(set(d.date() for d in dates))
    records = []
    for d in unique:
        dt = datetime.combine(d, datetime.min.time())
        records.append((
            int(d.strftime("%Y%m%d")), d,
            d.year, (d.month - 1) // 3 + 1, d.month, MONTHS[d.month - 1],
            dt.isocalendar()[1], d.weekday(), DAYS[d.weekday()],
            True, d.day == 1, (d + timedelta(days=1)).month != d.month,
        ))
    with conn.cursor() as cur:
        cur.executemany("""
            INSERT INTO dw.dim_date
                (date_id,full_date,year,quarter,month,month_name,
                 week_of_year,day_of_week,day_name,is_trading_day,
                 is_month_start,is_month_end)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (date_id) DO NOTHING
        """, records)
    conn.commit()
    log.info(f"dim_date: {len(records)} fechas procesadas")
