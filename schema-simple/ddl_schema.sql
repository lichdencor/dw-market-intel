-- =============================================================
-- Star Schema — yfinance + SEC Form 4 (Insider Trading)
-- PostgreSQL Monolítico (sin Citus)
--
-- 2 Facts | 5 Dimensiones | Con Foreign Keys & Constraints
--
-- Grain:
--   fact_price_daily    → ticker × día de trading (yfinance)
--   fact_insider_daily  → ticker × día × insider × tipo (SEC Form 4)
--
-- Ejecución:
--   psql -U postgres -d dw_analytics -f star_schema_postgresql.sql
-- =============================================================

-- Confirmar que estamos en la BD correcta
BEGIN;

-- =============================================================
-- 0. Schemas
-- =============================================================
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dw;

-- =============================================================
-- STAGING — tablas raw sin constraints, tipado mínimo
-- Truncadas y recargadas en cada run del pipeline
-- =============================================================

-- Raw OHLCV de yfinance
DROP TABLE IF EXISTS staging.raw_ohlcv CASCADE;
CREATE TABLE staging.raw_ohlcv (
    ticker          TEXT NOT NULL,
    trade_date      TEXT NOT NULL,       -- string YYYY-MM-DD del CSV
    open            TEXT,
    high            TEXT,
    low             TEXT,
    close           TEXT,
    adj_close       TEXT,
    volume          TEXT,
    loaded_at       TIMESTAMPTZ DEFAULT now(),
    
    -- Para staging, un índice único para deduplicación
    UNIQUE (ticker, trade_date)
);

CREATE INDEX idx_staging_raw_ohlcv_ticker ON staging.raw_ohlcv(ticker);
CREATE INDEX idx_staging_raw_ohlcv_date ON staging.raw_ohlcv(trade_date);

-- Raw Form 4 de SEC EDGAR — una fila por transacción XML parseada
DROP TABLE IF EXISTS staging.raw_insider CASCADE;
CREATE TABLE staging.raw_insider (
    -- Identificadores del filing
    accession_number    TEXT NOT NULL,       -- ej: 0001234567-24-000123
    filing_date         TEXT NOT NULL,       -- fecha en que la SEC recibió el Form 4

    -- Identificadores de las partes
    issuer_cik          TEXT NOT NULL,       -- CIK de la empresa emisora
    issuer_ticker       TEXT NOT NULL,       -- symbol: AAPL, MSFT, etc.
    issuer_name         TEXT,
    reporter_cik        TEXT NOT NULL,       -- CIK del insider (reporting owner)
    reporter_name       TEXT NOT NULL,
    reporter_title      TEXT,                -- cargo: CEO, CFO, Director, etc.
    is_director         TEXT,                -- '1' / '0'
    is_officer          TEXT,
    is_ten_pct_owner    TEXT,

    -- Detalle de la transacción (tabla II del Form 4)
    transaction_date    TEXT NOT NULL,       -- fecha efectiva de la transacción
    transaction_code    TEXT NOT NULL,       -- P=compra, S=venta, A=award, etc.
    security_title      TEXT,                -- ej: 'Common Stock', 'Stock Option'
    shares              TEXT,                -- cantidad de acciones
    price_per_share     TEXT,                -- precio por acción (null en awards)
    acquired_disposed   TEXT,                -- 'A' (acquired) / 'D' (disposed)
    shares_owned_after  TEXT,                -- posición total después de la transacción

    loaded_at           TIMESTAMPTZ DEFAULT now(),
    
    -- Clave única por filing
    UNIQUE (accession_number, issuer_cik, reporter_cik, transaction_date, transaction_code)
);

CREATE INDEX idx_staging_raw_insider_ticker ON staging.raw_insider(issuer_ticker);
CREATE INDEX idx_staging_raw_insider_cik ON staging.raw_insider(issuer_cik);
CREATE INDEX idx_staging_raw_insider_reporter ON staging.raw_insider(reporter_cik);
CREATE INDEX idx_staging_raw_insider_date ON staging.raw_insider(transaction_date);

-- =============================================================
-- DIMENSIONES — Con Primary Keys, Unique Constraints, Índices
-- =============================================================

-- =====================================================================
-- dim_ticker
-- Fuente: yfinance Ticker.info
-- Compartida entre fact_price_daily y fact_insider_daily
-- =====================================================================
DROP TABLE IF EXISTS dw.dim_ticker CASCADE;
CREATE TABLE dw.dim_ticker (
    ticker_id           SERIAL PRIMARY KEY,
    
    -- Identificadores únicos
    symbol              TEXT NOT NULL UNIQUE,
    sec_cik             TEXT UNIQUE,                     -- permite linkear con Form 4
    
    -- Información básica
    company_name        TEXT,
    sector              TEXT,
    industry            TEXT,
    exchange            TEXT,
    currency            TEXT DEFAULT 'USD',
    country             TEXT,
    
    -- Categorización
    market_cap_cat      TEXT,                            -- 'large', 'mid', 'small'
    
    -- Auditoría
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now(),
    
    -- Constraints de validación
    CHECK (symbol ~ '^[A-Z0-9]{1,5}$'),                  -- tickers son 1-5 caracteres uppercase
    CHECK (currency IN ('USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'CNY'))
);

CREATE INDEX idx_dim_ticker_symbol ON dw.dim_ticker(symbol);
CREATE INDEX idx_dim_ticker_cik ON dw.dim_ticker(sec_cik);
CREATE INDEX idx_dim_ticker_sector ON dw.dim_ticker(sector);

-- =====================================================================
-- dim_date
-- Fuente: generada en ETL a partir del rango de fechas
-- Compartida entre fact_price_daily y fact_insider_daily
-- =====================================================================
DROP TABLE IF EXISTS dw.dim_date CASCADE;
CREATE TABLE dw.dim_date (
    date_id             INT PRIMARY KEY,        -- formato YYYYMMDD, ej: 20240315
    full_date           DATE NOT NULL UNIQUE,
    year                SMALLINT NOT NULL,
    quarter             SMALLINT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    month               SMALLINT NOT NULL CHECK (month BETWEEN 1 AND 12),
    month_name          TEXT NOT NULL,
    week_of_year        SMALLINT NOT NULL CHECK (week_of_year BETWEEN 1 AND 53),
    day_of_week         SMALLINT NOT NULL CHECK (day_of_week BETWEEN 0 AND 6),    -- 0=lunes, 6=domingo
    day_name            TEXT NOT NULL,
    is_trading_day      BOOLEAN DEFAULT TRUE,
    is_month_start      BOOLEAN DEFAULT FALSE,
    is_month_end        BOOLEAN DEFAULT FALSE,
    
    created_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_dim_date_full_date ON dw.dim_date(full_date);
CREATE INDEX idx_dim_date_year_month ON dw.dim_date(year, month);
CREATE INDEX idx_dim_date_is_trading ON dw.dim_date(is_trading_day) WHERE is_trading_day = TRUE;

-- =====================================================================
-- dim_period
-- Granularidad de los datos (daily / weekly / monthly)
-- =====================================================================
DROP TABLE IF EXISTS dw.dim_period CASCADE;
CREATE TABLE dw.dim_period (
    period_id           SERIAL PRIMARY KEY,
    granularity         TEXT NOT NULL UNIQUE,   -- '1d', '1wk', '1mo'
    label               TEXT NOT NULL,          -- 'Daily', 'Weekly', 'Monthly'
    trading_days        SMALLINT NOT NULL CHECK (trading_days > 0),
    
    created_at          TIMESTAMPTZ DEFAULT now()
);

INSERT INTO dw.dim_period (granularity, label, trading_days) VALUES
    ('1d',  'Daily',   1),
    ('1wk', 'Weekly',  5),
    ('1mo', 'Monthly', 21)
ON CONFLICT (granularity) DO NOTHING;

-- =====================================================================
-- dim_insider [NUEVA]
-- Fuente: SEC Form 4 — reporting owner
-- Una fila por CIK (persona que reporta operaciones a la SEC)
-- =====================================================================
DROP TABLE IF EXISTS dw.dim_insider CASCADE;
CREATE TABLE dw.dim_insider (
    insider_id          SERIAL PRIMARY KEY,
    reporter_cik        TEXT NOT NULL UNIQUE,   -- CIK único asignado por la SEC
    reporter_name       TEXT NOT NULL,
    
    -- Cargo más reciente visto en los filings
    title               TEXT,
    is_director         BOOLEAN DEFAULT FALSE,
    is_officer          BOOLEAN DEFAULT FALSE,
    is_ten_pct_owner    BOOLEAN DEFAULT FALSE,
    
    created_at          TIMESTAMPTZ DEFAULT now(),
    updated_at          TIMESTAMPTZ DEFAULT now(),
    
    -- Removido: el CHECK anterior era incorrecto (muchos CFOs/VPs son officers sin ser directors)
);

CREATE INDEX idx_dim_insider_name ON dw.dim_insider(reporter_name);
CREATE INDEX idx_dim_insider_cik ON dw.dim_insider(reporter_cik);

-- =====================================================================
-- dim_transaction_type [NUEVA]
-- Fuente: SEC Form 4 — transaction code (tabla II)
-- Catálogo fijo de códigos de transacción de la SEC
-- =====================================================================
DROP TABLE IF EXISTS dw.dim_transaction_type CASCADE;
CREATE TABLE dw.dim_transaction_type (
    transaction_type_id SERIAL PRIMARY KEY,
    code                TEXT NOT NULL UNIQUE,   -- código SEC de 1 letra
    label               TEXT NOT NULL,
    direction           TEXT NOT NULL CHECK (direction IN ('buy', 'sell', 'neutral')),
    is_open_market      BOOLEAN DEFAULT FALSE,  -- TRUE solo para P y S
    
    created_at          TIMESTAMPTZ DEFAULT now()
);

-- Catálogo completo de códigos Form 4 de la SEC
INSERT INTO dw.dim_transaction_type (code, label, direction, is_open_market) VALUES
    ('P', 'Open Market Purchase',           'buy',     TRUE),
    ('S', 'Open Market Sale',               'sell',    TRUE),
    ('A', 'Grant / Award',                  'buy',     FALSE),
    ('D', 'Sale to Issuer / Disposition',   'sell',    FALSE),
    ('F', 'Tax Withholding',                'sell',    FALSE),
    ('M', 'Option Exercise',                'buy',     FALSE),
    ('G', 'Gift',                           'neutral', FALSE),
    ('V', 'Voluntary Transaction',          'neutral', FALSE),
    ('J', 'Other Acquisition',              'buy',     FALSE),
    ('K', 'Equity Swap',                    'neutral', FALSE),
    ('C', 'Convertible Security Exercise',  'buy',     FALSE),
    ('E', 'Expiration of Short Position',   'neutral', FALSE),
    ('H', 'Expiration of Long Derivative',  'neutral', FALSE),
    ('I', 'Discretionary Transaction',      'neutral', FALSE),
    ('L', 'Small Acquisition',              'buy',     FALSE),
    ('O', 'Out-of-Money Option Exercise',   'buy',     FALSE),
    ('U', 'Disposition Due to Tender',      'sell',    FALSE),
    ('W', 'Acquisition by Will/Inheritance','buy',     FALSE),
    ('X', 'In-the-Money Option Exercise',   'buy',     FALSE),
    ('Z', 'Deposit into Voting Trust',      'neutral', FALSE)
ON CONFLICT (code) DO NOTHING;

-- =============================================================
-- FACTS — Con Foreign Keys, Primary Keys, Índices Apropiados
-- =============================================================

-- =====================================================================
-- fact_price_daily
-- Fuente: yfinance
-- Grain: 1 fila por (ticker × día de trading × período)
-- =====================================================================
DROP TABLE IF EXISTS dw.fact_price_daily CASCADE;
CREATE TABLE dw.fact_price_daily (
    fact_id             BIGSERIAL,
    
    -- Foreign Keys (Dimensiones conformadas)
    ticker_id           INT NOT NULL REFERENCES dw.dim_ticker(ticker_id) 
                            ON DELETE RESTRICT ON UPDATE CASCADE,
    date_id             INT NOT NULL REFERENCES dw.dim_date(date_id) 
                            ON DELETE RESTRICT ON UPDATE CASCADE,
    period_id           INT NOT NULL DEFAULT 1 REFERENCES dw.dim_period(period_id) 
                            ON DELETE RESTRICT ON UPDATE CASCADE,

    -- OHLCV raw de yfinance
    open                NUMERIC(12,4) CHECK (open > 0),
    high                NUMERIC(12,4) CHECK (high > 0),
    low                 NUMERIC(12,4) CHECK (low > 0),
    close               NUMERIC(12,4) CHECK (close > 0),
    adj_close           NUMERIC(12,4) CHECK (adj_close > 0),
    volume              BIGINT CHECK (volume >= 0),

    -- Validación: high >= low, high >= close, low <= close
    CHECK (high >= low AND high >= close AND close >= low),

    -- Métricas calculadas en ETL
    daily_return        NUMERIC(12,4),              -- adj_close - prev_adj_close
    pct_change          NUMERIC(8,6),               -- (adj_close / prev_adj_close) - 1
    rolling_30d_vol     NUMERIC(8,6) CHECK (rolling_30d_vol >= 0),

    -- Auditoría
    loaded_at           TIMESTAMPTZ DEFAULT now(),

    -- Clave primaria compuesta
    PRIMARY KEY (ticker_id, date_id, period_id),
    
    -- Índices para queries comunes
    UNIQUE (fact_id)  -- para referencias futuras si es necesario
);

-- Índices para análisis de precios
CREATE INDEX idx_fact_price_ticker_date 
    ON dw.fact_price_daily(ticker_id, date_id DESC);
CREATE INDEX idx_fact_price_date 
    ON dw.fact_price_daily(date_id DESC);
CREATE INDEX idx_fact_price_high 
    ON dw.fact_price_daily(high DESC);
CREATE INDEX idx_fact_price_low 
    ON dw.fact_price_daily(low DESC);
CREATE INDEX idx_fact_price_volume 
    ON dw.fact_price_daily(volume DESC) WHERE volume > 0;

-- =====================================================================
-- fact_insider_daily [NUEVA]
-- Fuente: SEC EDGAR Form 4
-- Grain: 1 fila por (ticker × fecha_transaccion × insider × tipo)
--
-- Diseño de agregación:
--   Si un insider hace múltiples operaciones del mismo tipo
--   el mismo día, se consolidan: shares se suma, price es
--   promedio ponderado. El accession_number del último filing
--   procesado se guarda para trazabilidad hacia el XML original.
-- =====================================================================
DROP TABLE IF EXISTS dw.fact_insider_daily CASCADE;
CREATE TABLE dw.fact_insider_daily (
    fact_id             BIGSERIAL,
    
    -- Foreign Keys (Dimensiones conformadas)
    ticker_id           INT NOT NULL REFERENCES dw.dim_ticker(ticker_id) 
                            ON DELETE RESTRICT ON UPDATE CASCADE,
    date_id             INT NOT NULL REFERENCES dw.dim_date(date_id) 
                            ON DELETE RESTRICT ON UPDATE CASCADE,
    insider_id          INT NOT NULL REFERENCES dw.dim_insider(insider_id) 
                            ON DELETE RESTRICT ON UPDATE CASCADE,
    transaction_type_id INT NOT NULL REFERENCES dw.dim_transaction_type(transaction_type_id) 
                            ON DELETE RESTRICT ON UPDATE CASCADE,

    -- Métricas de la transacción
    shares_total        NUMERIC(18,4) NOT NULL CHECK (shares_total > 0),
    price_per_share     NUMERIC(12,4) CHECK (price_per_share IS NULL OR price_per_share > 0),
    total_value         NUMERIC(20,4) CHECK (total_value IS NULL OR total_value > 0),
    shares_owned_after  NUMERIC(18,4) CHECK (shares_owned_after >= 0),

    -- Trazabilidad al filing XML original
    accession_number    TEXT NOT NULL,           -- número de filing en EDGAR
    filing_date_id      INT NOT NULL REFERENCES dw.dim_date(date_id) 
                            ON DELETE RESTRICT ON UPDATE CASCADE,

    -- Auditoría
    loaded_at           TIMESTAMPTZ DEFAULT now(),

    -- Clave primaria compuesta
    PRIMARY KEY (ticker_id, date_id, insider_id, transaction_type_id),
    
    -- Índice único para referencias futuras
    UNIQUE (fact_id)
);

-- Índices para queries comunes de insider trading
CREATE INDEX idx_insider_ticker_date 
    ON dw.fact_insider_daily(ticker_id, date_id DESC);
CREATE INDEX idx_insider_date 
    ON dw.fact_insider_daily(date_id DESC);
CREATE INDEX idx_insider_id 
    ON dw.fact_insider_daily(insider_id);
CREATE INDEX idx_insider_transaction_type 
    ON dw.fact_insider_daily(transaction_type_id);
CREATE INDEX idx_insider_accession 
    ON dw.fact_insider_daily(accession_number);
CREATE INDEX idx_insider_shares_owned 
    ON dw.fact_insider_daily(shares_owned_after DESC) WHERE shares_owned_after > 0;

-- Índice combinado para análisis de actividad insider por empresa
CREATE INDEX idx_insider_ticker_insider_date 
    ON dw.fact_insider_daily(ticker_id, insider_id, date_id DESC);

-- =============================================================
-- VISTAS AUXILIARES PARA ANÁLISIS
-- =============================================================

-- Vista: Últimas transacciones insider
DROP VIEW IF EXISTS dw.v_latest_insider_transactions CASCADE;
CREATE VIEW dw.v_latest_insider_transactions AS
SELECT 
    t.symbol,
    i.reporter_name,
    i.title,
    tt.label as transaction_type,
    d.full_date,
    f.shares_total,
    f.price_per_share,
    f.total_value,
    f.shares_owned_after,
    f.accession_number
FROM dw.fact_insider_daily f
INNER JOIN dw.dim_ticker t ON f.ticker_id = t.ticker_id
INNER JOIN dw.dim_insider i ON f.insider_id = i.insider_id
INNER JOIN dw.dim_transaction_type tt ON f.transaction_type_id = tt.transaction_type_id
INNER JOIN dw.dim_date d ON f.date_id = d.date_id
ORDER BY d.full_date DESC
LIMIT 100;

-- Vista: Actividad insider por empresa (últimos 30 días)
DROP VIEW IF EXISTS dw.v_insider_activity_by_ticker CASCADE;
CREATE VIEW dw.v_insider_activity_by_ticker AS
SELECT 
    t.symbol,
    t.company_name,
    COUNT(DISTINCT f.insider_id) as unique_insiders,
    SUM(f.shares_total) as total_shares_traded,
    SUM(f.total_value) FILTER (WHERE tt.direction = 'buy') as buy_volume,
    SUM(f.total_value) FILTER (WHERE tt.direction = 'sell') as sell_volume,
    MAX(d.full_date) as last_activity_date
FROM dw.fact_insider_daily f
INNER JOIN dw.dim_ticker t ON f.ticker_id = t.ticker_id
INNER JOIN dw.dim_date d ON f.date_id = d.date_id
INNER JOIN dw.dim_transaction_type tt ON f.transaction_type_id = tt.transaction_type_id
WHERE d.full_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY t.symbol, t.company_name
ORDER BY last_activity_date DESC;

-- Vista: Combinada Price + Insider en un solo lugar
DROP VIEW IF EXISTS dw.v_price_with_insider_activity CASCADE;
CREATE VIEW dw.v_price_with_insider_activity AS
SELECT 
    d.full_date,
    t.symbol,
    fp.close,
    fp.volume,
    COALESCE(COUNT(DISTINCT fi.insider_id), 0) as insider_transactions_count,
    COALESCE(SUM(fi.shares_total), 0) as total_insider_shares,
    COALESCE(SUM(fi.total_value) FILTER (WHERE tt.direction = 'buy'), 0) as insider_buy_value,
    COALESCE(SUM(fi.total_value) FILTER (WHERE tt.direction = 'sell'), 0) as insider_sell_value
FROM dw.fact_price_daily fp
INNER JOIN dw.dim_ticker t ON fp.ticker_id = t.ticker_id
INNER JOIN dw.dim_date d ON fp.date_id = d.date_id
LEFT JOIN dw.fact_insider_daily fi ON fp.ticker_id = fi.ticker_id 
    AND fp.date_id = fi.date_id
LEFT JOIN dw.dim_transaction_type tt ON fi.transaction_type_id = tt.transaction_type_id
GROUP BY d.full_date, t.symbol, fp.close, fp.volume;

-- =============================================================
-- PROCEDURES PARA MANTENIMIENTO
-- =============================================================

-- Función para verificar integridad referencial
DROP FUNCTION IF EXISTS dw.check_referential_integrity();
CREATE FUNCTION dw.check_referential_integrity()
RETURNS TABLE (
    check_name TEXT,
    status TEXT,
    count_issues INT
) AS $$
BEGIN
    -- Chequear fact_price_daily sin dim_ticker
    RETURN QUERY
    SELECT 
        'fact_price_daily → dim_ticker'::TEXT,
        CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'FALTAN REFERENCES' END,
        COUNT(*)::INT
    FROM dw.fact_price_daily fp
    LEFT JOIN dw.dim_ticker dt ON fp.ticker_id = dt.ticker_id
    WHERE dt.ticker_id IS NULL;

    -- Chequear fact_insider_daily sin dim_ticker
    RETURN QUERY
    SELECT 
        'fact_insider_daily → dim_ticker'::TEXT,
        CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'FALTAN REFERENCES' END,
        COUNT(*)::INT
    FROM dw.fact_insider_daily fi
    LEFT JOIN dw.dim_ticker dt ON fi.ticker_id = dt.ticker_id
    WHERE dt.ticker_id IS NULL;

    -- Chequear fact_insider_daily sin dim_insider
    RETURN QUERY
    SELECT 
        'fact_insider_daily → dim_insider'::TEXT,
        CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'FALTAN REFERENCES' END,
        COUNT(*)::INT
    FROM dw.fact_insider_daily fi
    LEFT JOIN dw.dim_insider di ON fi.insider_id = di.insider_id
    WHERE di.insider_id IS NULL;

    -- Chequear fact_insider_daily sin dim_transaction_type
    RETURN QUERY
    SELECT 
        'fact_insider_daily → dim_transaction_type'::TEXT,
        CASE WHEN COUNT(*) = 0 THEN 'OK' ELSE 'FALTAN REFERENCES' END,
        COUNT(*)::INT
    FROM dw.fact_insider_daily fi
    LEFT JOIN dw.dim_transaction_type dtt ON fi.transaction_type_id = dtt.transaction_type_id
    WHERE dtt.transaction_type_id IS NULL;
END;
$$ LANGUAGE plpgsql;

-- =============================================================
-- RESUMEN FINAL
-- =============================================================
-- Ejecutar para verificar estructura:
-- SELECT * FROM dw.check_referential_integrity();

-- Contar registros:
-- SELECT 'dim_ticker' as table_name, COUNT(*) as row_count FROM dw.dim_ticker
-- UNION ALL SELECT 'dim_date', COUNT(*) FROM dw.dim_date
-- UNION ALL SELECT 'dim_insider', COUNT(*) FROM dw.dim_insider
-- UNION ALL SELECT 'dim_transaction_type', COUNT(*) FROM dw.dim_transaction_type
-- UNION ALL SELECT 'fact_price_daily', COUNT(*) FROM dw.fact_price_daily
-- UNION ALL SELECT 'fact_insider_daily', COUNT(*) FROM dw.fact_insider_daily;

COMMIT;
