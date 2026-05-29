"""
Recolección de datos — Yahoo Finance + SEC EDGAR Form 4 → MinIO
===============================================================
Responsabilidad única: descargar datos crudos de dos fuentes
y guardarlos como CSV en MinIO. No toca Postgres. No transforma.

Fuentes:
  1. Yahoo Finance  → ohlcv_raw.csv + ticker_info.csv (incluye sec_cik)
  2. SEC EDGAR Form 4 → insider_raw.csv

Output en MinIO:
  raw-ohlcv/{YYYY-MM-DD}/ohlcv_raw.csv
  raw-ohlcv/{YYYY-MM-DD}/ticker_info.csv
  raw-ohlcv/{YYYY-MM-DD}/insider_raw.csv

Variables de entorno:
  MINIO_ENDPOINT   default: localhost:9000
  MINIO_ACCESS_KEY default: minioadmin
  MINIO_SECRET_KEY default: minioadmin
  MINIO_BUCKET     default: raw-ohlcv
  TICKERS          default: lista hardcodeada de 30 tech
  PERIOD_DAYS      default: 365
"""

import io
import logging
import os
import re
import time
import random
import xml.etree.ElementTree as ET
from datetime import date, timedelta

import pandas as pd
import requests
import yfinance as yf
from minio import Minio

# =============================================================
# CONFIGURACIÓN
# =============================================================

MINIO_CONFIG = {
    "endpoint": os.getenv("MINIO_ENDPOINT", "10.10.10.20:9000"),
    "access_key": os.getenv("MINIO_ACCESS_KEY", "admin"),
    "secret_key": os.getenv("MINIO_SECRET_KEY", "admin1234"),
    "secure": False,
    "bucket": os.getenv("MINIO_BUCKET", "raw-ohlcv"),
}

PERIOD_DAYS      = int(os.getenv("PERIOD_DAYS",      "365"))
BATCH_SIZE       = int(os.getenv("BATCH_SIZE",      "0"))     # 0 = todos los tickers
TICKER_OFFSET    = int(os.getenv("TICKER_OFFSET",   "0"))     # índice de inicio del batch
INITIAL_SLEEP    = int(os.getenv("INITIAL_SLEEP",   "0"))     # pausa anti-burst
RL_WAIT_BASE     = int(os.getenv("RL_WAIT",         "60"))    # backoff de rate limit
SEC_ONLY         = os.getenv("SEC_ONLY",   "false").lower() == "true"
OHLCV_ONLY       = os.getenv("OHLCV_ONLY", "false").lower() == "true"
MAX_FILINGS      = int(os.getenv("MAX_FILINGS",     "500"))   # cap por ticker, evita AAPL→10000
GRANULARITY    = "1d"

# User-Agent requerido por la SEC — identifica al cliente
# https://www.sec.gov/os/accessing-edgar-data
SEC_HEADERS = {
    "User-Agent": "Lucas Chavez lucas.chavez@comunidad.ub.edu.ar",
    "Accept-Encoding": "gzip, deflate",
    "Host": "efts.sec.gov",
}

SEC_API_BASE      = "https://data.sec.gov"   # API endpoints: submissions, company_tickers
SEC_ARCHIVES_BASE = "https://www.sec.gov"    # Filing documents: /Archives/edgar/...
SEC_BASE          = SEC_API_BASE             # backwards compat — usado en get_cik_map
SEC_SEARCH        = "https://efts.sec.gov/LATEST/search-index"

# Pausa entre requests a la SEC — máximo permitido: 10 req/seg
SEC_SLEEP = 0.15

DEFAULT_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA",
    "META", "TSLA", "AVGO", "ORCL", "AMD",
    "CSCO", "INTC", "QCOM", "TXN", "IBM",
    "NOW", "CRM", "ADBE", "INTU", "SNOW",
    "PLTR", "UBER", "LYFT", "SHOP", "SQ",
    "PYPL", "NET", "DDOG", "CRWD", "ZS",
]

_all_tickers = os.getenv("TICKERS", ",".join(DEFAULT_TICKERS)).split(",")
if BATCH_SIZE > 0:
    TICKERS = _all_tickers[TICKER_OFFSET : TICKER_OFFSET + BATCH_SIZE]
else:
    TICKERS = _all_tickers

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [collector] %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)


# =============================================================
# MINIO — utilidades comunes
# =============================================================


def get_minio_client() -> Minio:
    client = Minio(
        MINIO_CONFIG["endpoint"],
        access_key=MINIO_CONFIG["access_key"],
        secret_key=MINIO_CONFIG["secret_key"],
        secure=MINIO_CONFIG["secure"],
    )
    bucket = MINIO_CONFIG["bucket"]
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        log.info(f"Bucket '{bucket}' creado")
    return client


def upload_csv(client: Minio, df: pd.DataFrame, filename: str) -> None:
    bucket = MINIO_CONFIG["bucket"]
    partition = date.today().isoformat()
    object_name = f"{partition}/{filename}"

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    client.put_object(
        bucket,
        object_name,
        data=io.BytesIO(csv_bytes),
        length=len(csv_bytes),
        content_type="text/csv",
    )
    log.info(f"Subido → s3://{bucket}/{object_name} ({len(csv_bytes) / 1024:.1f} KB)")


# Claves de deduplicación por archivo — usadas en modo append
_DEDUP_KEYS: dict = {
    "ohlcv_raw.csv":   ["ticker", "trade_date"],
    "ticker_info.csv": ["symbol"],
    "insider_raw.csv": ["accession_number", "reporter_cik", "transaction_date", "transaction_code"],
}


def read_csv_from_minio_safe(client: Minio, filename: str) -> pd.DataFrame | None:
    """Lee un CSV de la partición de hoy. Retorna None si no existe."""
    try:
        bucket = MINIO_CONFIG["bucket"]
        object_name = f"{date.today().isoformat()}/{filename}"
        response = client.get_object(bucket, object_name)
        return pd.read_csv(io.BytesIO(response.read()))
    except Exception:
        return None


def upload_csv_append(client: Minio, df: pd.DataFrame, filename: str) -> None:
    """
    Append al CSV de hoy en MinIO, deduplicando por las claves del archivo.
    Usado en modo batch para acumular resultados de múltiples runs del día.
    """
    existing = read_csv_from_minio_safe(client, filename)
    if existing is not None and not existing.empty:
        df = pd.concat([existing, df], ignore_index=True)
        keys = _DEDUP_KEYS.get(filename, [])
        valid_keys = [k for k in keys if k in df.columns]
        if valid_keys:
            df = df.drop_duplicates(subset=valid_keys, keep="last")
    upload_csv(client, df, filename)


# =============================================================
# YAHOO FINANCE — OHLCV
# =============================================================


def _make_yf_session():
    """Session con cache y User-Agent de browser para evitar rate limiting."""
    try:
        import requests_cache
        session = requests_cache.CachedSession(
            cache_name="/tmp/yf_cache",
            expire_after=3600,
        )
    except ImportError:
        import requests
        session = requests.Session()
    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    })
    return session


def download_ohlcv() -> pd.DataFrame:
    """
    Descarga OHLCV con yf.download() batch — más eficiente y menos propenso
    a rate limiting que llamadas individuales por ticker.
    Usa requests-cache + User-Agent de browser para mayor confiabilidad.
    """
    end = date.today()
    start = end - timedelta(days=PERIOD_DAYS)
    log.info(f"Descargando OHLCV batch: {len(TICKERS)} tickers | {start} → {end}")

    if INITIAL_SLEEP > 0:
        log.info(f"  Pausa inicial {INITIAL_SLEEP}s...")
        time.sleep(INITIAL_SLEEP)

    session = _make_yf_session()
    raw = None

    for attempt in range(5):
        try:
            raw = yf.download(
                TICKERS,
                start=start,
                end=end,
                interval=GRANULARITY,
                auto_adjust=False,
                group_by="ticker",
                threads=False,
                progress=False,
            )
            if raw is not None and not raw.empty:
                break
        except Exception as e:
            msg = str(e).lower()
            if "rate" in msg or "429" in msg or "too many" in msg:
                wait = RL_WAIT_BASE * (attempt + 1) + random.uniform(5, 15)
                log.warning(f"  Rate limit (intento {attempt+1}/5), esperando {wait:.0f}s...")
                time.sleep(wait)
            else:
                log.warning(f"  Intento {attempt+1}/5: {e}")
                time.sleep(5 + random.random())

    if raw is None or raw.empty:
        log.error("No se pudo descargar OHLCV después de 5 reintentos")
        return pd.DataFrame()

    frames = []
    for ticker in TICKERS:
        try:
            # yf.download con group_by="ticker" devuelve MultiIndex incluso con 1 ticker en lista
            df = raw[ticker].copy() if len(TICKERS) > 1 else raw.copy()
            df = df.dropna(subset=["Close"])
            if df.empty:
                log.warning(f"  {ticker}: sin datos")
                continue
            df["ticker"] = ticker
            df.index.name = "trade_date"
            df = df.reset_index()
            df.columns = [c.lower().replace(" ", "_") for c in df.columns]
            frames.append(df)
            log.info(f"  {ticker}: {len(df)} días")
        except KeyError:
            log.warning(f"  {ticker}: sin datos en el batch")

    result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    log.info(f"OHLCV total: {len(result):,} registros | {len(frames)}/{len(TICKERS)} tickers")
    return result


# =============================================================
# YAHOO FINANCE — TICKER INFO
# =============================================================


def download_ticker_info(cik_map: dict = None) -> pd.DataFrame:
    """
    Descarga metadata de tickers via yfinance.
    Si se pasa cik_map, incluye sec_cik en el output para linkear con Form 4.
    """
    log.info("Descargando metadata de tickers...")
    if cik_map is None:
        cik_map = {}

    records = []
    for symbol in TICKERS:
        success = False
        for attempt in range(3):
            try:
                info = yf.Ticker(symbol).info
                cap = info.get("marketCap", 0) or 0
                records.append(
                    {
                        "symbol": symbol,
                        "company_name": info.get("longName", symbol),
                        "sector": info.get("sector", "Unknown"),
                        "industry": info.get("industry", "Unknown"),
                        "exchange": info.get("exchange", "Unknown"),
                        "currency": info.get("currency", "USD"),
                        "country": info.get("country", "Unknown"),
                        "market_cap": cap,
                        "market_cap_cat": (
                            "large" if cap >= 10_000_000_000
                            else "mid" if cap >= 2_000_000_000
                            else "small"
                        ),
                        "sec_cik": cik_map.get(symbol),
                    }
                )
                log.info(f"  OK {symbol}")
                success = True
                break
            except Exception as e:
                log.warning(f"  {symbol} intento {attempt + 1} falló: {e}")
                time.sleep(2 + random.random())

        if not success:
            log.warning(f"  Fallback {symbol}")
            records.append(
                {
                    "symbol": symbol,
                    "company_name": symbol,
                    "sector": "Unknown",
                    "industry": "Unknown",
                    "exchange": "Unknown",
                    "currency": "USD",
                    "country": "Unknown",
                    "market_cap": 0,
                    "market_cap_cat": "small",
                    "sec_cik": cik_map.get(symbol),
                }
            )

        time.sleep(1.5)

    df = pd.DataFrame(records)
    log.info(f"Metadata descargada: {len(df)} tickers")
    return df


# =============================================================
# SEC EDGAR — utilidades HTTP
# =============================================================


def sec_get(url: str, params: dict = None, retries: int = 3) -> requests.Response | None:
    """
    GET a la SEC con rate limiting y reintentos.
    Respeta el límite de 10 req/seg con SEC_SLEEP entre llamadas.
    El header Host se omite siempre — requests lo setea automáticamente
    con el host correcto de la URL, y enviarlo mal causa 403.
    """
    headers = {k: v for k, v in SEC_HEADERS.items() if k != "Host"}
    for attempt in range(retries):
        try:
            time.sleep(SEC_SLEEP)
            r = requests.get(url, headers=headers, params=params, timeout=15)
            if r.status_code == 200:
                return r
            elif r.status_code == 429:
                wait = 60 + random.uniform(5, 15)
                log.warning(f"  Rate limit SEC (429), esperando {wait:.0f}s...")
                time.sleep(wait)
            else:
                log.warning(f"  SEC HTTP {r.status_code} en {url}")
                time.sleep(2 ** attempt)
        except Exception as e:
            log.warning(f"  Error SEC request intento {attempt + 1}: {e}")
            time.sleep(3 + random.random())
    return None


# =============================================================
# SEC EDGAR — CIK lookup
# =============================================================


def get_cik_map(tickers: list) -> dict:
    """
    Descarga el mapa completo ticker → CIK de la SEC.
    Retorna dict {symbol: cik_str_con_ceros} ej: {'AAPL': '0000320193'}
    """
    log.info("Descargando mapa ticker → CIK de la SEC...")
    # Nota: el archivo está en www.sec.gov, no en data.sec.gov
    url = "https://www.sec.gov/files/company_tickers.json"

    headers = {k: v for k, v in SEC_HEADERS.items() if k != "Host"}
    r = None
    for attempt in range(3):
        try:
            time.sleep(SEC_SLEEP)
            r = requests.get(url, headers=headers, timeout=15)
            if r.status_code == 200:
                break
        except Exception as e:
            log.warning(f"  CIK map intento {attempt + 1}: {e}")
            time.sleep(3 + random.random())

    if r is None or r.status_code != 200:
        log.error("No se pudo obtener el mapa CIK de la SEC")
        return {}

    data = r.json()
    cik_map = {}
    for entry in data.values():
        symbol = entry.get("ticker", "").upper()
        if symbol in tickers:
            cik_map[symbol] = str(entry["cik_str"]).zfill(10)

    found = len(cik_map)
    missing = [t for t in tickers if t not in cik_map]
    log.info(f"  CIKs encontrados: {found}/{len(tickers)}")
    if missing:
        log.warning(f"  Sin CIK: {missing}")
    return cik_map


# =============================================================
# SEC EDGAR — buscar filings Form 4 por CIK
# =============================================================


def get_form4_filings(cik: str, ticker: str, start_date: str, end_date: str) -> list:
    """
    Busca todos los filings Form 4 de una empresa en el período.
    Usa la API de búsqueda full-text de EDGAR.
    """
    filings = []
    from_idx = 0
    page_size = 100

    while True:
        params = {
            "category": "form-type",
            "forms": "4",
            "dateRange": "custom",
            "startdt": start_date,
            "enddt": end_date,
            "entity": cik,
            "_source": "file_date,period_of_report,file_num",
            "from": from_idx,
            "size": page_size,
        }

        r = sec_get(SEC_SEARCH, params=params)
        if r is None:
            break

        data = r.json()
        hits = data.get("hits", {}).get("hits", [])
        if not hits:
            break

        for hit in hits:
            src = hit.get("_source", {})
            # _id puede venir como "0001234567-24-000123:document.xml"
            # tomamos solo la parte del accession number (antes del ":")
            acc = hit.get("_id", "").split(":")[0].strip()
            if not acc:
                continue
            acc_nodash = acc.replace("-", "")
            cik_nodash = cik.lstrip("0")
            index_url = (
                f"{SEC_ARCHIVES_BASE}/Archives/edgar/data/"
                f"{cik_nodash}/{acc_nodash}/{acc}-index.htm"
            )
            filings.append(
                {
                    "accession_number": acc,
                    "filing_date": src.get("file_date", ""),
                    "index_url": index_url,
                    "cik": cik,
                    "ticker": ticker,
                }
            )

        if len(hits) < page_size:
            break
        from_idx += page_size

    return filings


# =============================================================
# SEC EDGAR — parsear XML de un Form 4
# =============================================================


def get_xml_url(index_url: str) -> str | None:
    """Desde la página índice del filing, encuentra la URL del XML principal."""
    r = sec_get(index_url)
    if r is None:
        return None

    matches = re.findall(r'href="(/Archives/edgar/data/[^"]+\.xml)"', r.text)
    for match in matches:
        # Saltar rutas con /xsl* (XSLT rendering en HTML, no el XML puro)
        if "/xsl" not in match.lower():
            return f"https://www.sec.gov{match}"
    return None


def parse_form4_xml(
    xml_content: str, accession_number: str, filing_date: str, ticker: str
) -> list:
    """
    Parsea el XML crudo de un Form 4 y extrae todas las transacciones
    de la Tabla II (non-derivative securities).
    """
    records = []
    try:
        root = ET.fromstring(xml_content)
    except ET.ParseError as e:
        log.warning(f"  XML parse error en {accession_number}: {e}")
        return []

    def txt(element, path: str) -> str:
        node = element.find(path)
        return (node.text or "").strip() if node is not None else ""

    issuer_cik = txt(root, "issuer/issuerCik")
    issuer_name = txt(root, "issuer/issuerName")
    issuer_ticker = txt(root, "issuer/issuerTradingSymbol") or ticker

    owner = root.find("reportingOwner")
    if owner is None:
        return []

    reporter_cik = txt(owner, "reportingOwnerId/rptOwnerCik")
    reporter_name = txt(owner, "reportingOwnerId/rptOwnerName")
    reporter_title = txt(owner, "reportingOwnerRelationship/officerTitle")
    is_director = txt(owner, "reportingOwnerRelationship/isDirector")
    is_officer = txt(owner, "reportingOwnerRelationship/isOfficer")
    is_10pct = txt(owner, "reportingOwnerRelationship/isTenPercentOwner")

    for txn in root.findall(".//nonDerivativeTransaction"):
        records.append(
            {
                "accession_number": accession_number,
                "filing_date": filing_date,
                "issuer_cik": issuer_cik,
                "issuer_ticker": issuer_ticker,
                "issuer_name": issuer_name,
                "reporter_cik": reporter_cik,
                "reporter_name": reporter_name,
                "reporter_title": reporter_title,
                "is_director": is_director,
                "is_officer": is_officer,
                "is_ten_pct_owner": is_10pct,
                "transaction_date": txt(txn, "transactionDate/value"),
                "transaction_code": txt(txn, "transactionCoding/transactionCode"),
                "security_title": txt(txn, "securityTitle/value"),
                "shares": txt(txn, "transactionAmounts/transactionShares/value"),
                "price_per_share": txt(
                    txn, "transactionAmounts/transactionPricePerShare/value"
                ),
                "acquired_disposed": txt(
                    txn, "transactionAmounts/transactionAcquiredDisposedCode/value"
                ),
                "shares_owned_after": txt(
                    txn, "postTransactionAmounts/sharesOwnedFollowingTransaction/value"
                ),
            }
        )

    return records


# =============================================================
# SEC EDGAR — orquestador principal
# =============================================================


def download_insider_trading() -> pd.DataFrame:
    """
    Descarga todos los Form 4 de los 30 tickers en el período y
    retorna un DataFrame con una fila por transacción cruda.
    """
    end_date = date.today().isoformat()
    start_date = (date.today() - timedelta(days=PERIOD_DAYS)).isoformat()

    log.info("=" * 55)
    log.info(f"SEC EDGAR Form 4 | {start_date} → {end_date}")
    log.info("=" * 55)

    cik_map = get_cik_map(TICKERS)
    if not cik_map:
        log.error("Sin CIKs, abortando descarga de insider trading")
        return pd.DataFrame()

    all_records = []
    total_filings = 0
    total_txns = 0

    for ticker, cik in cik_map.items():
        log.info(f"[{ticker}] CIK={cik} — buscando Form 4...")

        filings = get_form4_filings(cik, ticker, start_date, end_date)
        if len(filings) > MAX_FILINGS:
            log.warning(f"  {len(filings)} filings → truncando a {MAX_FILINGS} (MAX_FILINGS)")
            filings = filings[:MAX_FILINGS]
        log.info(f"  {len(filings)} filings a procesar")
        total_filings += len(filings)

        ticker_txns = 0
        for filing in filings:
            xml_url = get_xml_url(filing["index_url"])
            if xml_url is None:
                log.warning(f"  Sin XML para {filing['accession_number']}, saltando")
                continue

            r = sec_get(xml_url)
            if r is None:
                continue

            records = parse_form4_xml(
                r.text,
                filing["accession_number"],
                filing["filing_date"],
                ticker,
            )
            all_records.extend(records)
            ticker_txns += len(records)

        log.info(f"  {ticker_txns} transacciones extraídas")
        total_txns += ticker_txns

    log.info(f"Total: {total_filings} filings | {total_txns} transacciones")

    if not all_records:
        log.warning("No se obtuvieron transacciones de insider trading")
        return pd.DataFrame()

    return pd.DataFrame(all_records)


# =============================================================
# MAIN
# =============================================================


def main():
    batch_mode = BATCH_SIZE > 0
    mode_label = f"batch {TICKER_OFFSET}–{TICKER_OFFSET + len(TICKERS) - 1}" if batch_mode else "full"

    log.info("=" * 55)
    log.info(f"Collector — {mode_label} | {len(TICKERS)} tickers → MinIO")
    log.info("=" * 55)

    # En modo batch usamos append para acumular en la partición del día
    _upload = upload_csv_append if batch_mode else upload_csv

    client = get_minio_client()
    cik_map = get_cik_map(TICKERS) if not OHLCV_ONLY else {}

    # --- Yahoo Finance --- (skip si SEC_ONLY)
    if not SEC_ONLY:
        log.info("--- Yahoo Finance ---")
        df_ohlcv = download_ohlcv()
        if not df_ohlcv.empty:
            _upload(client, df_ohlcv, "ohlcv_raw.csv")

        df_info = download_ticker_info(cik_map=cik_map)
        if not df_info.empty:
            _upload(client, df_info, "ticker_info.csv")
    else:
        log.info("--- Yahoo Finance OMITIDA (SEC_ONLY=true) ---")

    # --- SEC EDGAR Form 4 --- (skip si OHLCV_ONLY)
    if not OHLCV_ONLY:
        log.info("--- SEC EDGAR Form 4 ---")
        df_insider = download_insider_trading()
        if not df_insider.empty:
            _upload(client, df_insider, "insider_raw.csv")
    else:
        log.info("--- SEC EDGAR OMITIDA (OHLCV_ONLY=true) ---")

    log.info("=" * 55)
    log.info(f"Recolección completada ({mode_label})")
    log.info("=" * 55)


if __name__ == "__main__":
    main()
