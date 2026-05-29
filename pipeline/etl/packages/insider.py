import io
import logging

import pandas as pd

from .base import ETLPackage, PackageResult
from .utils import get_minio_client, read_csv, populate_dim_date
from .profiler import generate_report

log = logging.getLogger(__name__)

INSIDER_REQUIRED = {"reporter_cik", "transaction_date", "transaction_code", "issuer_ticker"}


class InsiderPackage(ETLPackage):
    """
    Fuente: SEC EDGAR Form 4 (insider_raw.csv)
    Carga: staging.raw_insider → dim_insider, dim_date → fact_insider_daily
    Nueva columna: source_partition
    """

    def extract(self) -> pd.DataFrame:
        import os
        client = get_minio_client(self.minio_config)
        bucket = self.minio_config["bucket"]
        try:
            df = read_csv(client, bucket, self.partition_date, "insider_raw.csv")
        except Exception as e:
            log.warning(f"insider_raw.csv no encontrado ({e}) — pipeline insider omitido")
            return pd.DataFrame()

        # ETL incremental: solo transacciones cuyo accession_number no esté ya cargado
        if os.getenv("INCREMENTAL", "true").lower() != "false" and not df.empty \
                and "accession_number" in df.columns:
            with self.conn.cursor() as cur:
                cur.execute("SELECT DISTINCT accession_number FROM dw.fact_insider_daily")
                loaded = {row[0] for row in cur.fetchall()}

            if loaded:
                before = len(df)
                df = df[~df["accession_number"].isin(loaded)]
                skipped = before - len(df)
                if skipped > 0:
                    self.log.info(f"Incremental: {skipped:,} transacciones ya cargadas omitidas, {len(df):,} nuevas")

        return df

    def profile(self, data: pd.DataFrame) -> list[str]:
        # Schema contract
        missing = INSIDER_REQUIRED - set(data.columns)
        if missing:
            raise ValueError(f"[schema contract] Columnas faltantes en insider_raw: {missing}")

        return generate_report(
            data, "insider",
            conn=self.conn, run_id=self._run_id
        )

    def stage(self, data: pd.DataFrame) -> None:
        # Filas con transaction_code inválido o reporter_cik vacío → quarantine
        bad_code = ~data["transaction_code"].astype(str).str.match(r"^[A-Z]$", na=False)
        bad_cik  = data["reporter_cik"].astype(str).isin(["", "nan", "None"])
        mask_bad = bad_code | bad_cik

        if mask_bad.any():
            quarantine = data[mask_bad].copy()
            quarantine["reason"] = "transaction_code invalido o reporter_cik vacio"
            _to_quarantine(self.conn, quarantine, "insider")
            log.warning(f"  [quarantine] {mask_bad.sum()} filas → staging.quarantine_insider")
            data = data[~mask_bad]

        cols = [
            "accession_number","filing_date","issuer_cik","issuer_ticker","issuer_name",
            "reporter_cik","reporter_name","reporter_title","is_director","is_officer",
            "is_ten_pct_owner","transaction_date","transaction_code","security_title",
            "shares","price_per_share","acquired_disposed","shares_owned_after",
        ]
        df_s = data.reindex(columns=cols).fillna("").astype(str)
        # Deduplicar por la PK del staging antes del COPY
        dedup_keys = ["accession_number","issuer_cik","reporter_cik","transaction_date","transaction_code"]
        before = len(df_s)
        df_s = df_s.drop_duplicates(subset=dedup_keys)
        if len(df_s) < before:
            log.warning(f"  [stage] {before - len(df_s)} duplicados eliminados del CSV")
        buf = io.StringIO()
        df_s.to_csv(buf, index=False, header=False)
        buf.seek(0)
        with self.conn.cursor() as cur:
            cur.execute("TRUNCATE staging.raw_insider")
            cur.copy_expert(
                f"COPY staging.raw_insider ({','.join(cols)}) FROM STDIN WITH CSV", buf
            )
        self.conn.commit()
        log.info(f"staging.raw_insider: {len(df_s):,} filas")

    def load(self) -> None:
        self._load_dim_date()
        self._upsert_dim_insider()
        self._load_fact()

    def _load_dim_date(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute("SELECT transaction_date, filing_date FROM staging.raw_insider")
            rows = cur.fetchall()
        dates = [r[0] for r in rows] + [r[1] for r in rows]
        populate_dim_date(self.conn, dates)

    def _upsert_dim_insider(self) -> None:
        sql = """
        INSERT INTO dw.dim_insider
            (reporter_cik,reporter_name,title,is_director,is_officer,is_ten_pct_owner)
        SELECT DISTINCT ON (reporter_cik)
            reporter_cik, reporter_name,
            NULLIF(TRIM(reporter_title),'') AS title,
            (is_director='1'), (is_officer='1'), (is_ten_pct_owner='1')
        FROM staging.raw_insider
        WHERE reporter_cik NOT IN ('','nan','None')
          AND reporter_name NOT IN ('','nan','None')
        ORDER BY reporter_cik, filing_date DESC
        ON CONFLICT (reporter_cik) DO UPDATE SET
            reporter_name=EXCLUDED.reporter_name,
            title=COALESCE(EXCLUDED.title, dw.dim_insider.title),
            is_director=EXCLUDED.is_director,
            is_officer=EXCLUDED.is_officer,
            is_ten_pct_owner=EXCLUDED.is_ten_pct_owner,
            updated_at=now()
        """
        with self.conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.rowcount
        self.conn.commit()
        log.info(f"dim_insider: {rows} insiders")

    def _load_fact(self) -> None:
        partition = self.partition_date
        sql = f"""
        WITH clean AS (
            SELECT UPPER(TRIM(issuer_ticker)) AS ticker,
                TRIM(reporter_cik) AS reporter_cik,
                TRIM(transaction_code) AS tx_code,
                TRIM(transaction_date) AS tx_date,
                TRIM(filing_date) AS filing_date,
                accession_number,
                CASE WHEN shares ~ '^[0-9]+(\\.[0-9]+)?$' THEN shares::NUMERIC ELSE NULL END AS shares,
                CASE WHEN price_per_share ~ '^[0-9]+(\\.[0-9]+)?$' THEN price_per_share::NUMERIC ELSE NULL END AS price,
                CASE WHEN shares_owned_after ~ '^[0-9]+(\\.[0-9]+)?$' THEN shares_owned_after::NUMERIC ELSE NULL END AS owned_after
            FROM staging.raw_insider
            WHERE transaction_date ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'
              AND filing_date ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}$'
              AND transaction_code ~ '^[A-Z]$'
              AND reporter_cik NOT IN ('','nan','None')
              AND shares ~ '^[0-9]+(\\.[0-9]+)?$'
        ),
        agg AS (
            SELECT ticker, reporter_cik, tx_code,
                TO_CHAR(TO_DATE(tx_date,'YYYY-MM-DD'),'YYYYMMDD')::INT AS date_id,
                TO_CHAR(TO_DATE(MAX(filing_date),'YYYY-MM-DD'),'YYYYMMDD')::INT AS filing_date_id,
                MAX(accession_number) AS accession_number,
                SUM(shares) AS shares_total,
                CASE WHEN SUM(CASE WHEN price IS NOT NULL THEN shares ELSE 0 END) > 0
                    THEN SUM(COALESCE(price*shares,0)) / SUM(CASE WHEN price IS NOT NULL THEN shares ELSE 0 END)
                    ELSE NULL END AS price_per_share,
                MAX(owned_after) AS shares_owned_after
            FROM clean WHERE shares > 0
            GROUP BY ticker, reporter_cik, tx_code, tx_date
        )
        INSERT INTO dw.fact_insider_daily
            (ticker_id,date_id,insider_id,transaction_type_id,
             shares_total,price_per_share,total_value,shares_owned_after,
             accession_number,filing_date_id,source_partition)
        SELECT t.ticker_id, a.date_id, i.insider_id, tt.transaction_type_id,
            a.shares_total, a.price_per_share,
            CASE WHEN a.price_per_share IS NOT NULL
                THEN ROUND(a.shares_total * a.price_per_share, 4) ELSE NULL END,
            a.shares_owned_after, a.accession_number, a.filing_date_id,
            '{partition}'::DATE
        FROM agg a
        JOIN dw.dim_ticker           t  ON t.symbol         = a.ticker
        JOIN dw.dim_insider          i  ON i.reporter_cik   = a.reporter_cik
        JOIN dw.dim_transaction_type tt ON tt.code          = a.tx_code
        ON CONFLICT (ticker_id,date_id,insider_id,transaction_type_id) DO UPDATE SET
            shares_total=EXCLUDED.shares_total,
            price_per_share=EXCLUDED.price_per_share,
            total_value=EXCLUDED.total_value,
            shares_owned_after=EXCLUDED.shares_owned_after,
            accession_number=EXCLUDED.accession_number,
            filing_date_id=EXCLUDED.filing_date_id,
            source_partition=EXCLUDED.source_partition
        """
        with self.conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.rowcount
        self.conn.commit()
        log.info(f"fact_insider_daily: {rows:,} filas")


def _to_quarantine(conn, df: pd.DataFrame, source: str) -> None:
    try:
        table = f"staging.quarantine_{source}"
        cols = [c for c in df.columns if c != "reason"]
        buf = io.StringIO()
        df[cols + ["reason"]].astype(str).to_csv(buf, index=False, header=False)
        buf.seek(0)
        with conn.cursor() as cur:
            cur.copy_expert(
                f"COPY {table} ({','.join(cols)},reason) FROM STDIN WITH CSV", buf
            )
        conn.commit()
    except Exception as e:
        log.warning(f"[quarantine] No se pudo escribir en {table}: {e}")
