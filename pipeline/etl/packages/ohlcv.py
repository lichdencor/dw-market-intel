import io
import logging

import pandas as pd

from .base import ETLPackage, PackageResult
from .utils import get_minio_client, read_csv, populate_dim_date
from .profiler import generate_report

log = logging.getLogger(__name__)

# Columnas requeridas del CSV source
OHLCV_REQUIRED = {"ticker", "trade_date", "close", "adj_close", "volume"}


class OHLCVPackage(ETLPackage):
    """
    Fuente: yfinance (ohlcv_raw.csv + ticker_info.csv)
    Carga: staging.raw_ohlcv → dim_ticker, dim_date → fact_price_daily
    Nuevas columnas: source_partition, volume_ratio, is_suspect
    """

    def extract(self) -> dict:
        client = get_minio_client(self.minio_config)
        bucket = self.minio_config["bucket"]
        return {
            "ohlcv": read_csv(client, bucket, self.partition_date, "ohlcv_raw.csv"),
            "info":  read_csv(client, bucket, self.partition_date, "ticker_info.csv"),
        }

    def profile(self, data: dict) -> list[str]:
        # Schema contract: verificar columnas requeridas
        missing = OHLCV_REQUIRED - set(data["ohlcv"].columns)
        if missing:
            raise ValueError(f"[schema contract] Columnas faltantes en ohlcv_raw: {missing}")

        warnings = generate_report(
            data["ohlcv"], "ohlcv",
            conn=self.conn, run_id=self._run_id
        )
        n = data["info"]["symbol"].nunique() if "symbol" in data["info"].columns else 0
        if n < 20:
            warnings.append(f"ticker_info: solo {n} tickers (esperado 30)")
        return warnings

    def stage(self, data: dict) -> None:
        # Filas con close <= 0 o ticker vacío van a quarantine
        df = data["ohlcv"].copy()
        bad_close = df["close"].apply(
            lambda v: pd.to_numeric(v, errors="coerce")
        ).le(0).fillna(False)
        bad_ticker = df["ticker"].astype(str).isin(["", "nan", "None"])
        mask_bad = bad_close | bad_ticker

        if mask_bad.any():
            quarantine = df[mask_bad].copy()
            quarantine["reason"] = "close <= 0 or ticker invalido"
            _to_quarantine(self.conn, quarantine, "ohlcv")
            log.warning(f"  [quarantine] {mask_bad.sum()} filas → staging.quarantine_ohlcv")
            df = df[~mask_bad]

        cols = ["ticker","trade_date","open","high","low","close","adj_close","volume"]
        df_s = df.reindex(columns=cols).astype(str)
        buf = io.StringIO()
        df_s.to_csv(buf, index=False, header=False)
        buf.seek(0)
        with self.conn.cursor() as cur:
            cur.execute("TRUNCATE staging.raw_ohlcv")
            cur.copy_expert(
                f"COPY staging.raw_ohlcv ({','.join(cols)}) FROM STDIN WITH CSV", buf
            )
        self.conn.commit()
        log.info(f"staging.raw_ohlcv: {len(df_s):,} filas")

    def load(self) -> None:
        self._load_dim_date()
        self._load_fact()

    def _load_dim_date(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute("SELECT trade_date FROM staging.raw_ohlcv")
            dates = [r[0] for r in cur.fetchall()]
        populate_dim_date(self.conn, dates)

    def _load_fact(self) -> None:
        partition = self.partition_date
        sql_temp = f"""
        CREATE TEMP TABLE _tmp_ohlcv ON COMMIT DROP AS
        WITH base AS (
            SELECT t.ticker_id, TO_DATE(s.trade_date,'YYYY-MM-DD') AS trade_date,
                s.open::NUMERIC, s.high::NUMERIC, s.low::NUMERIC,
                s.close::NUMERIC, s.adj_close::NUMERIC, s.volume::NUMERIC::BIGINT
            FROM staging.raw_ohlcv s
            JOIN dw.dim_ticker t ON t.symbol = s.ticker
            WHERE s.adj_close NOT IN ('nan','None','') AND s.close NOT IN ('nan','None','')
        ),
        with_lags AS (
            SELECT *, LAG(adj_close) OVER (PARTITION BY ticker_id ORDER BY trade_date) AS prev_adj
            FROM base
        ),
        with_pct AS (
            SELECT *, adj_close - prev_adj AS daily_return,
                (adj_close / NULLIF(prev_adj,0)) - 1 AS pct_change
            FROM with_lags
        ),
        with_vol AS (
            SELECT *,
                STDDEV(pct_change) OVER (
                    PARTITION BY ticker_id ORDER BY trade_date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ) AS rolling_30d_vol,
                volume / NULLIF(AVG(volume::NUMERIC) OVER (
                    PARTITION BY ticker_id ORDER BY trade_date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                ), 0) AS volume_ratio
            FROM with_pct
        ),
        with_indicators AS (
            SELECT *,
                -- Simple Moving Averages
                AVG(close) OVER (PARTITION BY ticker_id ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)  AS sma_20,
                AVG(close) OVER (PARTITION BY ticker_id ORDER BY trade_date
                    ROWS BETWEEN 49 PRECEDING AND CURRENT ROW)  AS sma_50,
                AVG(close) OVER (PARTITION BY ticker_id ORDER BY trade_date
                    ROWS BETWEEN 199 PRECEDING AND CURRENT ROW) AS sma_200,
                -- Bollinger Bands (SMA20 ± 2σ)
                AVG(close) OVER (PARTITION BY ticker_id ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
                + 2 * STDDEV(close) OVER (PARTITION BY ticker_id ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS bb_upper,
                AVG(close) OVER (PARTITION BY ticker_id ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW)
                - 2 * STDDEV(close) OVER (PARTITION BY ticker_id ORDER BY trade_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW) AS bb_lower
            FROM with_vol
        )
        SELECT DISTINCT ON (ticker_id, date_id)
            ticker_id, TO_CHAR(trade_date,'YYYYMMDD')::INT AS date_id, 1 AS period_id,
            open, high, low, close, adj_close, volume,
            daily_return, pct_change, rolling_30d_vol, volume_ratio,
            ABS(COALESCE(pct_change, 0)) > 0.15 AS is_suspect,
            sma_20, sma_50, sma_200, bb_upper, bb_lower,
            '{partition}'::DATE AS source_partition
        FROM with_indicators
        ORDER BY ticker_id, date_id
        """
        sql_ins = """
        INSERT INTO dw.fact_price_daily
            (ticker_id,date_id,period_id,open,high,low,close,adj_close,volume,
             daily_return,pct_change,rolling_30d_vol,volume_ratio,is_suspect,
             sma_20,sma_50,sma_200,bb_upper,bb_lower,source_partition)
        SELECT ticker_id,date_id,period_id,open,high,low,close,adj_close,volume,
            daily_return,pct_change,rolling_30d_vol,volume_ratio,is_suspect,
            sma_20,sma_50,sma_200,bb_upper,bb_lower,source_partition
        FROM _tmp_ohlcv
        ON CONFLICT (ticker_id,date_id,period_id) DO UPDATE SET
            open=EXCLUDED.open, high=EXCLUDED.high, low=EXCLUDED.low,
            close=EXCLUDED.close, adj_close=EXCLUDED.adj_close, volume=EXCLUDED.volume,
            daily_return=EXCLUDED.daily_return, pct_change=EXCLUDED.pct_change,
            rolling_30d_vol=EXCLUDED.rolling_30d_vol, volume_ratio=EXCLUDED.volume_ratio,
            is_suspect=EXCLUDED.is_suspect,
            sma_20=EXCLUDED.sma_20, sma_50=EXCLUDED.sma_50, sma_200=EXCLUDED.sma_200,
            bb_upper=EXCLUDED.bb_upper, bb_lower=EXCLUDED.bb_lower,
            source_partition=EXCLUDED.source_partition
        """
        with self.conn.cursor() as cur:
            cur.execute(sql_temp)
            cur.execute(sql_ins)
            rows = cur.rowcount
        self.conn.commit()
        log.info(f"fact_price_daily: {rows:,} filas")

    # ------------------------------------------------------------------ run override
    def run(self) -> PackageResult:
        log.info("=== OHLCVPackage ===")
        self._run_id = self._start_run()
        try:
            data = self.extract()
            if data["ohlcv"].empty:
                log.warning("Sin datos OHLCV — package omitido")
                self._finish_run(status="skipped")
                return PackageResult(name="OHLCVPackage", success=True, rows_loaded=0)

            warnings = self.profile(data)
            for w in warnings:
                log.warning(f"  [profile] {w}")

            self.stage(data)
            self._upsert_dim_ticker(data["info"])
            self._load_dim_date()
            self._load_fact()
            self._populate_monthly_snapshot()

            rows = len(data["ohlcv"])
            self._finish_run(rows_extracted=rows, rows_loaded=rows,
                             warnings_count=len(warnings), status="ok")
            log.info(f"=== OHLCVPackage OK — {rows:,} filas ===")
            return PackageResult(name="OHLCVPackage", success=True,
                                 rows_loaded=rows, warnings=warnings)
        except Exception as e:
            self._finish_run(status="failed", error_msg=str(e))
            raise

    def _upsert_dim_ticker(self, df_info: pd.DataFrame) -> None:
        """
        Upsert con lógica SCD Type 2 simplificada:
        si cambia sector, industry o market_cap_cat, archiva la versión anterior
        en dim_ticker_history antes de actualizar.
        """
        # Atributos que disparan el archivado histórico si cambian
        SCD2_COLS = ("sector", "industry", "market_cap_cat")
        archived = 0

        with self.conn.cursor() as cur:
            for _, row in df_info.iterrows():
                sec_cik = row.get("sec_cik")
                if pd.isna(sec_cik) if pd.api.types.is_scalar(sec_cik) else False:
                    sec_cik = None
                symbol = row["symbol"]

                # Leer versión actual para detectar cambios (SCD2)
                cur.execute("""
                    SELECT ticker_id, sector, industry, market_cap_cat,
                           company_name, exchange, sec_cik, created_at::DATE
                    FROM dw.dim_ticker WHERE symbol = %s
                """, (symbol,))
                existing = cur.fetchone()

                if existing:
                    tid, old_sec, old_ind, old_cap, old_name, old_exc, old_cik, old_date = existing
                    new_vals = (row.get("sector"), row.get("industry"), row.get("market_cap_cat"))
                    old_vals = (old_sec, old_ind, old_cap)

                    if new_vals != old_vals:
                        changed = [c for c, o, n in zip(SCD2_COLS, old_vals, new_vals) if o != n]
                        cur.execute("""
                            INSERT INTO dw.dim_ticker_history
                                (ticker_id,symbol,company_name,sector,industry,
                                 exchange,market_cap_cat,sec_cik,
                                 valid_from,valid_to,change_reason)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_DATE,%s)
                        """, (tid, symbol, old_name, old_sec, old_ind,
                              old_exc, old_cap, old_cik,
                              old_date, f"changed: {', '.join(changed)}"))
                        archived += 1

                cur.execute("""
                    INSERT INTO dw.dim_ticker
                        (symbol,company_name,sector,industry,exchange,
                         currency,country,market_cap_cat,sec_cik)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (symbol) DO UPDATE SET
                        company_name=EXCLUDED.company_name, sector=EXCLUDED.sector,
                        industry=EXCLUDED.industry, market_cap_cat=EXCLUDED.market_cap_cat,
                        sec_cik=COALESCE(EXCLUDED.sec_cik, dw.dim_ticker.sec_cik),
                        updated_at=now()
                """, (symbol, row.get("company_name"), row.get("sector"),
                      row.get("industry"), row.get("exchange"),
                      row.get("currency","USD"), row.get("country"),
                      row.get("market_cap_cat"), sec_cik))

        self.conn.commit()
        if archived:
            log.info(f"dim_ticker_history: {archived} versiones archivadas (SCD2)")
        log.info(f"dim_ticker: {len(df_info)} tickers actualizados")

    def _populate_monthly_snapshot(self) -> None:
        """Puebla fact_price_monthly_snapshot desde fact_price_daily (upsert).
        Usa DISTINCT ON para el primer y último día del mes, GROUP BY para los agregados.
        """
        partition = self.partition_date
        sql = f"""
        WITH monthly_agg AS (
            SELECT f.ticker_id,
                EXTRACT(YEAR  FROM d.full_date)::SMALLINT AS year,
                EXTRACT(MONTH FROM d.full_date)::SMALLINT AS month,
                MAX(f.high)              AS high_price,
                MIN(f.low)               AS low_price,
                SUM(f.volume)            AS total_volume,
                AVG(f.rolling_30d_vol)   AS avg_volatility,
                COUNT(*)                 AS trading_days
            FROM dw.fact_price_daily f
            JOIN dw.dim_date d ON d.date_id = f.date_id
            GROUP BY f.ticker_id,
                EXTRACT(YEAR  FROM d.full_date),
                EXTRACT(MONTH FROM d.full_date)
        ),
        first_day AS (
            SELECT DISTINCT ON (f.ticker_id, EXTRACT(YEAR FROM d.full_date), EXTRACT(MONTH FROM d.full_date))
                f.ticker_id,
                EXTRACT(YEAR  FROM d.full_date)::SMALLINT AS year,
                EXTRACT(MONTH FROM d.full_date)::SMALLINT AS month,
                f.close     AS open_price,
                f.adj_close AS first_adj
            FROM dw.fact_price_daily f
            JOIN dw.dim_date d ON d.date_id = f.date_id
            ORDER BY f.ticker_id,
                EXTRACT(YEAR FROM d.full_date),
                EXTRACT(MONTH FROM d.full_date),
                d.full_date ASC
        ),
        last_day AS (
            SELECT DISTINCT ON (f.ticker_id, EXTRACT(YEAR FROM d.full_date), EXTRACT(MONTH FROM d.full_date))
                f.ticker_id,
                EXTRACT(YEAR  FROM d.full_date)::SMALLINT AS year,
                EXTRACT(MONTH FROM d.full_date)::SMALLINT AS month,
                f.close     AS close_price,
                f.adj_close AS last_adj
            FROM dw.fact_price_daily f
            JOIN dw.dim_date d ON d.date_id = f.date_id
            ORDER BY f.ticker_id,
                EXTRACT(YEAR FROM d.full_date),
                EXTRACT(MONTH FROM d.full_date),
                d.full_date DESC
        )
        INSERT INTO dw.fact_price_monthly_snapshot
            (ticker_id,year,month,open_price,close_price,high_price,low_price,
             total_volume,monthly_return,avg_volatility,trading_days,source_partition)
        SELECT
            a.ticker_id, a.year, a.month,
            fd.open_price, ld.close_price,
            a.high_price, a.low_price, a.total_volume,
            (ld.last_adj / NULLIF(fd.first_adj, 0)) - 1 AS monthly_return,
            a.avg_volatility, a.trading_days,
            '{partition}'::DATE
        FROM monthly_agg a
        JOIN first_day fd ON fd.ticker_id=a.ticker_id AND fd.year=a.year AND fd.month=a.month
        JOIN last_day  ld ON ld.ticker_id=a.ticker_id AND ld.year=a.year AND ld.month=a.month
        ON CONFLICT (ticker_id,year,month) DO UPDATE SET
            open_price=EXCLUDED.open_price, close_price=EXCLUDED.close_price,
            high_price=EXCLUDED.high_price, low_price=EXCLUDED.low_price,
            total_volume=EXCLUDED.total_volume, monthly_return=EXCLUDED.monthly_return,
            avg_volatility=EXCLUDED.avg_volatility, trading_days=EXCLUDED.trading_days,
            source_partition=EXCLUDED.source_partition
        """
        with self.conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.rowcount
        self.conn.commit()
        log.info(f"fact_price_monthly_snapshot: {rows:,} snapshots actualizados")


def _to_quarantine(conn, df: pd.DataFrame, source: str) -> None:
    """Inserta filas rechazadas en la tabla de quarantine correspondiente."""
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
