-- =============================================================
--  Metabase — Questions y Dashboard
--  Schema: dw | DB: datawarehouse (PostgreSQL monolítico)
--
--  Usar en Metabase → New → SQL Query (editor nativo, no builder)
-- =============================================================


-- =============================================================
-- QUESTION 1 — Rendimiento acumulado del año (bar chart)
-- Visualización: Bar chart | X: symbol | Y: rendimiento_pct
-- Color: por sector
-- =============================================================

SELECT
    t.symbol,
    t.company_name,
    t.sector,
    ROUND(
        (
            LAST_VALUE(f.adj_close) OVER (
                PARTITION BY f.ticker_id
                ORDER BY d.full_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )
            / NULLIF(
                FIRST_VALUE(f.adj_close) OVER (
                    PARTITION BY f.ticker_id
                    ORDER BY d.full_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ), 0
            ) - 1
        ) * 100, 2
    ) AS rendimiento_pct
FROM dw.fact_price_daily f
JOIN dw.dim_ticker t ON t.ticker_id = f.ticker_id
JOIN dw.dim_date   d ON d.date_id   = f.date_id
GROUP BY t.symbol, t.company_name, t.sector, f.ticker_id, f.adj_close, d.full_date
ORDER BY rendimiento_pct DESC;


-- =============================================================
-- QUESTION 2 — Serie temporal de precio (line chart)
-- Visualización: Line chart | X: full_date | Y: adj_close
-- Filtro Metabase: {{ticker}}
-- =============================================================

SELECT
    d.full_date   AS fecha,
    t.symbol,
    f.adj_close   AS precio_cierre_ajustado,
    f.volume      AS volumen
FROM dw.fact_price_daily f
JOIN dw.dim_ticker t ON t.ticker_id = f.ticker_id
JOIN dw.dim_date   d ON d.date_id   = f.date_id
WHERE t.symbol = {{ticker}}
ORDER BY d.full_date;


-- =============================================================
-- QUESTION 3 — Volatilidad anualizada (bar chart horizontal)
-- Visualización: Row chart | X: volatilidad_pct | Y: symbol
-- =============================================================

SELECT
    t.symbol,
    t.company_name,
    ROUND((STDDEV(f.pct_change) * SQRT(252) * 100)::numeric, 2) AS volatilidad_anual_pct
FROM dw.fact_price_daily f
JOIN dw.dim_ticker t ON t.ticker_id = f.ticker_id
WHERE f.pct_change IS NOT NULL
GROUP BY t.symbol, t.company_name
ORDER BY volatilidad_anual_pct DESC;


-- =============================================================
-- QUESTION 4 — Volatilidad rolling 30d en el tiempo (line chart)
-- Visualización: Line chart | X: fecha | Y: volatilidad_30d_pct
-- Variable: {{ticker}}
-- =============================================================

SELECT
    d.full_date                        AS fecha,
    t.symbol,
    ROUND(f.rolling_30d_vol * 100, 4) AS volatilidad_30d_pct
FROM dw.fact_price_daily f
JOIN dw.dim_ticker t ON t.ticker_id = f.ticker_id
JOIN dw.dim_date   d ON d.date_id   = f.date_id
WHERE t.symbol = {{ticker}}
  AND f.rolling_30d_vol IS NOT NULL
ORDER BY d.full_date;


-- =============================================================
-- QUESTION 5 — Máximos y mínimos mensuales (combo chart)
-- Visualización: Bar (rango) + Line (close promedio)
-- Variable: {{ticker}}
-- =============================================================

SELECT
    d.year,
    d.month,
    d.month_name,
    t.symbol,
    ROUND(MAX(f.high),  2) AS maximo_mensual,
    ROUND(MIN(f.low),   2) AS minimo_mensual,
    ROUND(AVG(f.close), 2) AS cierre_promedio
FROM dw.fact_price_daily f
JOIN dw.dim_ticker t ON t.ticker_id = f.ticker_id
JOIN dw.dim_date   d ON d.date_id   = f.date_id
WHERE t.symbol = {{ticker}}
GROUP BY d.year, d.month, d.month_name, t.symbol
ORDER BY d.year, d.month;


-- =============================================================
-- QUESTION 6 — Peores días del año (tabla con conditional formatting)
-- Celdas rojas en caída_pct < -3%
-- =============================================================

SELECT
    t.symbol,
    d.full_date                    AS fecha,
    ROUND(f.pct_change * 100, 2)  AS caida_pct,
    ROUND(f.close, 2)             AS cierre,
    f.volume
FROM dw.fact_price_daily f
JOIN dw.dim_ticker t ON t.ticker_id = f.ticker_id
JOIN dw.dim_date   d ON d.date_id   = f.date_id
WHERE f.pct_change IS NOT NULL
  AND f.pct_change < -0.03
ORDER BY f.pct_change ASC
LIMIT 30;


-- =============================================================
-- QUESTION 7 — Scatter: rendimiento vs volatilidad
-- Visualización: Scatter | X: volatilidad | Y: rendimiento | Bubble: symbol
-- =============================================================

WITH rendimiento AS (
    SELECT
        f.ticker_id,
        ROUND((
            (MAX(f.adj_close) - MIN(f.adj_close))
            / NULLIF(MIN(f.adj_close), 0) * 100
        )::numeric, 2) AS rendimiento_pct
    FROM dw.fact_price_daily f
    GROUP BY f.ticker_id
),
volatilidad AS (
    SELECT
        f.ticker_id,
        ROUND((STDDEV(f.pct_change) * SQRT(252) * 100)::numeric, 2) AS volatilidad_pct
    FROM dw.fact_price_daily f
    WHERE f.pct_change IS NOT NULL
    GROUP BY f.ticker_id
)
SELECT
    t.symbol,
    t.company_name,
    t.sector,
    r.rendimiento_pct,
    v.volatilidad_pct
FROM rendimiento r
JOIN volatilidad  v ON v.ticker_id  = r.ticker_id
JOIN dw.dim_ticker t ON t.ticker_id = r.ticker_id
ORDER BY r.rendimiento_pct DESC;


-- =============================================================
-- QUESTION 8 — Top insiders por volumen de compras (tabla)
-- Muestra quiénes compraron más en mercado abierto (código P)
-- Visualización: Table | ordenado por buy_value DESC
-- =============================================================

SELECT
    i.reporter_name,
    i.title,
    t.symbol,
    t.company_name,
    COUNT(*)                          AS operaciones,
    SUM(f.shares_total)               AS acciones_compradas,
    ROUND(SUM(f.total_value)::numeric, 2) AS valor_total_usd
FROM dw.fact_insider_daily f
JOIN dw.dim_insider          i  ON i.insider_id          = f.insider_id
JOIN dw.dim_ticker           t  ON t.ticker_id           = f.ticker_id
JOIN dw.dim_transaction_type tt ON tt.transaction_type_id = f.transaction_type_id
WHERE tt.code = 'P'   -- Open Market Purchase
GROUP BY i.reporter_name, i.title, t.symbol, t.company_name
ORDER BY valor_total_usd DESC NULLS LAST
LIMIT 20;


-- =============================================================
-- QUESTION 9 — Compras vs ventas de insiders por ticker (bar chart apilado)
-- Visualización: Stacked bar | X: symbol | Y: buy_value / sell_value
-- =============================================================

SELECT
    t.symbol,
    t.company_name,
    ROUND(
        COALESCE(SUM(f.total_value) FILTER (WHERE tt.direction = 'buy'), 0)::numeric, 2
    ) AS compras_usd,
    ROUND(
        COALESCE(SUM(f.total_value) FILTER (WHERE tt.direction = 'sell'), 0)::numeric, 2
    ) AS ventas_usd,
    ROUND(
        COALESCE(SUM(f.total_value) FILTER (WHERE tt.is_open_market = TRUE AND tt.direction = 'buy'), 0)::numeric, 2
    ) AS compras_mercado_abierto_usd
FROM dw.fact_insider_daily f
JOIN dw.dim_ticker           t  ON t.ticker_id           = f.ticker_id
JOIN dw.dim_transaction_type tt ON tt.transaction_type_id = f.transaction_type_id
GROUP BY t.symbol, t.company_name
ORDER BY compras_usd DESC;


-- =============================================================
-- QUESTION 10 — Timeline de actividad insider (line chart)
-- Número de transacciones por día en el período
-- Visualización: Line chart | X: fecha | Y: num_transacciones
-- Variable: {{ticker}}
-- =============================================================

SELECT
    d.full_date                                AS fecha,
    t.symbol,
    COUNT(*)                                   AS num_transacciones,
    SUM(f.shares_total)                        AS acciones_transaccionadas,
    COUNT(*) FILTER (WHERE tt.direction = 'buy')  AS compras,
    COUNT(*) FILTER (WHERE tt.direction = 'sell') AS ventas
FROM dw.fact_insider_daily f
JOIN dw.dim_ticker           t  ON t.ticker_id           = f.ticker_id
JOIN dw.dim_date             d  ON d.date_id             = f.date_id
JOIN dw.dim_transaction_type tt ON tt.transaction_type_id = f.transaction_type_id
WHERE t.symbol = {{ticker}}
GROUP BY d.full_date, t.symbol
ORDER BY d.full_date;


-- =============================================================
-- QUESTION 11 — Insiders con mayor posición accionaria (tabla)
-- Muestra la posición final de cada insider en cada empresa
-- Visualización: Table
-- =============================================================

SELECT
    t.symbol,
    t.company_name,
    i.reporter_name,
    i.title,
    ROUND(MAX(f.shares_owned_after)::numeric, 0) AS acciones_en_cartera,
    MAX(d.full_date)                              AS ultima_transaccion
FROM dw.fact_insider_daily f
JOIN dw.dim_ticker  t ON t.ticker_id  = f.ticker_id
JOIN dw.dim_insider i ON i.insider_id = f.insider_id
JOIN dw.dim_date    d ON d.date_id    = f.date_id
WHERE f.shares_owned_after IS NOT NULL
  AND f.shares_owned_after > 0
GROUP BY t.symbol, t.company_name, i.reporter_name, i.title
ORDER BY acciones_en_cartera DESC NULLS LAST
LIMIT 30;


-- =============================================================
-- QUESTION 12 — Precio + actividad insider combinada (combo chart)
-- Visualización: Line (precio) + Bar (insider buy value)
-- Variable: {{ticker}}
-- Fuente: dw.v_price_with_insider_activity (vista del schema)
-- =============================================================

SELECT
    full_date                        AS fecha,
    symbol,
    ROUND(close::numeric, 2)         AS precio_cierre,
    volume                           AS volumen,
    insider_transactions_count       AS transacciones_insider,
    ROUND(insider_buy_value::numeric, 2)  AS compras_insider_usd,
    ROUND(insider_sell_value::numeric, 2) AS ventas_insider_usd
FROM dw.v_price_with_insider_activity
WHERE symbol = {{ticker}}
ORDER BY full_date;


-- =============================================================
-- ESTRUCTURA DEL DASHBOARD SUGERIDA
-- =============================================================
--
--  ┌──────────────────────────────────────────────────────────┐
--  │  TECH SECTOR — ANÁLISIS DE MERCADO + INSIDER TRADING     │
--  ├────────────────────┬─────────────────────────────────────┤
--  │  Q1: Rendimiento   │  Q3: Volatilidad anualizada         │
--  │  acumulado (bar)   │  (row chart)                        │
--  ├────────────────────┴─────────────────────────────────────┤
--  │  Q7: Scatter riesgo/retorno (ancho completo)             │
--  ├────────────────────┬─────────────────────────────────────┤
--  │  Q2: Serie temporal│  Q4: Volatilidad rolling 30d        │
--  │  precio {{ticker}} │  {{ticker}}                         │
--  ├────────────────────┴─────────────────────────────────────┤
--  │  Q12: Precio + insider activity {{ticker}} (combo)       │
--  ├────────────────────┬─────────────────────────────────────┤
--  │  Q9: Compras vs    │  Q8: Top insiders por compras       │
--  │  ventas por ticker │  (tabla)                            │
--  ├────────────────────┴─────────────────────────────────────┤
--  │  Q10: Timeline actividad insider {{ticker}}              │
--  ├────────────────────┬─────────────────────────────────────┤
--  │  Q5: Max/min mens. │  Q6: Peores días del año            │
--  │  {{ticker}}        │  (tabla con celdas rojas)           │
--  └────────────────────┴─────────────────────────────────────┘
--
--  Las questions con {{ticker}} comparten el mismo filtro de
--  dashboard → un solo dropdown controla todas las cards.
-- =============================================================
