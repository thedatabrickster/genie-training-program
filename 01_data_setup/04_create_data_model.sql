-- Databricks SQL
-- =============================================================================
-- Genie Training: Star Schema Data Model
-- =============================================================================
-- This script creates:
--   Pre-joined views optimized for Genie spaces
--
-- Prerequisites:
--   - 02_load_stock_data.py has been run (dim_companies, dim_sectors,
--     fact_stock_prices, fact_sp500_benchmark, fact_financials)
--   - 03_load_economic_data.py has been run (fact_economic_indicators)
--
-- When run via DABs, the catalog and schema values are injected as
-- parameters using {{key}} syntax. For manual execution, find and replace
-- {{catalog}} with your catalog name and {{schema}} with your schema name.
-- =============================================================================

USE CATALOG IDENTIFIER({{catalog}});
USE SCHEMA IDENTIFIER({{schema}});

-- =============================================================================
-- 1. FACT TABLES — created by upstream notebooks
-- =============================================================================
-- fact_financials       → 02_load_stock_data.py  (yfinance income stmt / balance sheet / cash flow)
-- fact_economic_indicators → 03_load_economic_data.py (IMF WEO data from Volume)
-- fact_stock_prices     → 02_load_stock_data.py  (yfinance daily OHLCV)
-- fact_sp500_benchmark  → 02_load_stock_data.py  (yfinance ^GSPC)
-- dim_companies         → 02_load_stock_data.py  (yfinance company info)
-- dim_sectors           → 02_load_stock_data.py  (static reference)
--
-- This script only creates the pre-joined VIEWS for Genie.


-- =============================================================================
-- 2. PRE-JOINED VIEWS FOR GENIE
-- =============================================================================

CREATE OR REPLACE VIEW vw_company_fundamentals
COMMENT 'Combined view of company profiles with their annual financial data, dividend info, and valuation multiples. Use this view to answer questions about company revenue, profitability, dividends, valuation, sector comparisons, and financial health. One row per company per fiscal year.'
AS
SELECT
    c.ticker,
    c.company_name,
    c.sector,
    c.industry,
    c.headquarters_country,
    c.market_cap,
    c.employees,
    c.dividend_yield,
    c.dividend_rate,
    c.trailing_pe,
    c.forward_pe,
    c.price_to_book,
    c.payout_ratio,
    c.beta,
    c.fifty_two_week_high,
    c.fifty_two_week_low,
    f.fiscal_year,
    f.revenue,
    f.net_income,
    f.gross_profit,
    f.operating_income,
    f.operating_cash_flow,
    f.total_assets,
    f.total_liabilities,
    f.stockholders_equity,
    f.cash,
    f.long_term_debt,
    f.eps,
    f.gross_margin_pct,
    f.operating_margin_pct,
    f.net_margin_pct,
    f.return_on_assets_pct,
    f.return_on_equity_pct,
    f.current_ratio,
    f.debt_to_equity,
    f.debt_to_asset_ratio,
    f.debt_ratio_pct,
    f.cash_flow_margin_pct,
    f.revenue_growth_pct,
    f.net_income_growth_pct
FROM dim_companies c
LEFT JOIN fact_financials f ON c.ticker = f.ticker;


CREATE OR REPLACE VIEW vw_stock_performance
COMMENT 'Daily stock prices with company context. Use this view to answer questions about stock returns, price trends, trading volume, and sector-level market performance. One row per company per trading day.'
AS
SELECT
    c.ticker,
    c.company_name,
    c.sector,
    c.industry,
    c.market_cap,
    p.trade_date,
    p.open_price,
    p.high_price,
    p.low_price,
    p.close_price,
    p.volume,
    p.daily_return_pct,
    YEAR(p.trade_date) AS trade_year,
    MONTH(p.trade_date) AS trade_month,
    QUARTER(p.trade_date) AS trade_quarter
FROM dim_companies c
INNER JOIN fact_stock_prices p ON c.ticker = p.ticker;


CREATE OR REPLACE VIEW vw_stock_vs_benchmark
COMMENT 'Daily stock prices with S&P 500 benchmark data for relative performance analysis. Use this view to answer questions like "How did Apple perform relative to the S&P 500?" or "Which stocks outperformed the market?" One row per company per trading day.'
AS
SELECT
    c.ticker,
    c.company_name,
    c.sector,
    c.industry,
    c.beta,
    p.trade_date,
    p.close_price,
    p.daily_return_pct AS stock_daily_return_pct,
    b.close_price AS sp500_close,
    b.daily_return_pct AS sp500_daily_return_pct,
    p.daily_return_pct - b.daily_return_pct AS excess_return_pct,
    YEAR(p.trade_date) AS trade_year,
    MONTH(p.trade_date) AS trade_month,
    QUARTER(p.trade_date) AS trade_quarter
FROM dim_companies c
INNER JOIN fact_stock_prices p ON c.ticker = p.ticker
INNER JOIN fact_sp500_benchmark b ON p.trade_date = b.trade_date;


CREATE OR REPLACE VIEW vw_economic_overview
COMMENT 'Macroeconomic indicators for 20 major world economies sourced from the IMF World Economic Outlook. Covers GDP growth, inflation, unemployment, government debt, and current account balance from 2015 to 2030 (with projections flagged). Use this view to answer questions about GDP growth, inflation trends, unemployment, and government debt across countries and years.'
AS
SELECT
    country_code,
    country,
    indicator_code,
    indicator_name,
    year,
    value,
    is_projection
FROM fact_economic_indicators;


-- =============================================================================
-- 4. VALIDATION QUERIES
-- =============================================================================

SELECT 'dim_companies' AS object, COUNT(*) AS row_count FROM dim_companies
UNION ALL SELECT 'dim_sectors', COUNT(*) FROM dim_sectors
UNION ALL SELECT 'fact_financials', COUNT(*) FROM fact_financials
UNION ALL SELECT 'fact_stock_prices', COUNT(*) FROM fact_stock_prices
UNION ALL SELECT 'fact_sp500_benchmark', COUNT(*) FROM fact_sp500_benchmark
UNION ALL SELECT 'fact_economic_indicators', COUNT(*) FROM fact_economic_indicators
UNION ALL SELECT 'vw_company_fundamentals', COUNT(*) FROM vw_company_fundamentals
UNION ALL SELECT 'vw_stock_performance', COUNT(*) FROM vw_stock_performance
UNION ALL SELECT 'vw_stock_vs_benchmark', COUNT(*) FROM vw_stock_vs_benchmark
UNION ALL SELECT 'vw_economic_overview', COUNT(*) FROM vw_economic_overview;
