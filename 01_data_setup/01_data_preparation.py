# Databricks notebook source

# MAGIC %md
# MAGIC # Module 3: Data Preparation — Walkthrough
# MAGIC
# MAGIC **Databricks Genie Training Program**
# MAGIC
# MAGIC This notebook walks through the data pipeline for the Genie training program.
# MAGIC It covers:
# MAGIC
# MAGIC 1. **Pipeline architecture** — how the 4 steps fit together
# MAGIC 2. **What each notebook does** — stock data, economic data, views, grants
# MAGIC 3. **Data validation** — live queries to verify the pipeline output
# MAGIC 4. **Sample questions** — what this data enables in Genie
# MAGIC
# MAGIC > **Note:** This notebook is a **read-and-run walkthrough** — it queries the
# MAGIC > data that was loaded by the pipeline notebooks. It does not re-run the
# MAGIC > pipeline itself.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "training")
dbutils.widgets.text("schema", "genie")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")
print(f"Using: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Architecture
# MAGIC
# MAGIC The data pipeline has **4 steps**, orchestrated by a Databricks Asset Bundle job:
# MAGIC
# MAGIC ```
# MAGIC      ┌────────────────────────────────────────────┐
# MAGIC      │  Prerequisites: 00_setup_prerequisites.py │
# MAGIC      │  Catalog + Schema + Volume + File upload   │
# MAGIC      └────────────────────┬───────────────────────┘
# MAGIC                           │
# MAGIC           ┌───────────────┴───────────────┐
# MAGIC           ▼                               ▼
# MAGIC ┌─────────────────────────────┐  ┌──────────────────────────┐
# MAGIC │  Step 1: 02_load_stock_data.py │  │  Step 2: 03_load_economic_data.py   │
# MAGIC │  yfinance API               │  │  _data.py                │
# MAGIC │  ─────────────────────────  │  │  ────────────────────────│
# MAGIC │  • dim_companies            │  │  • fact_economic         │
# MAGIC │  • dim_sectors              │  │    _indicators           │
# MAGIC │  • fact_stock_prices        │  │  (IMF WEO from Volume)   │
# MAGIC │  • fact_sp500_benchmark     │  │                          │
# MAGIC │  • fact_financials          │  │                          │
# MAGIC └──────────────┬──────────────┘  └────────────┬─────────────┘
# MAGIC                │                              │
# MAGIC                ▼                              ▼
# MAGIC      ┌────────────────────────────────────────────┐
# MAGIC      │     Step 3: 04_create_data_model.sql       │
# MAGIC      │     4 pre-joined views for Genie           │
# MAGIC      └────────────────────────────────────────────┘
# MAGIC                │
# MAGIC                ▼
# MAGIC      ┌────────────────────────────────────────────┐
# MAGIC      │     Step 4: 05_grant_genie_access.py       │
# MAGIC      │     Unity Catalog SELECT grants            │
# MAGIC      └────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC - Steps 1 and 2 run **in parallel** (no shared dependencies)
# MAGIC - Step 3 depends on both completing
# MAGIC - Step 4 grants access to the consumer
# MAGIC - All steps run on **serverless compute**

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Stock Data (yfinance API)
# MAGIC
# MAGIC **`02_load_stock_data.py`** pulls everything from the yfinance API:
# MAGIC
# MAGIC | Output Table | Description |
# MAGIC |-------------|-------------|
# MAGIC | `dim_companies` | ~67 S&P 500 companies with sector, market cap, **dividend yield**, **P/E**, **P/B**, **beta**, 52-week range |
# MAGIC | `dim_sectors` | 11 GICS sector reference data |
# MAGIC | `fact_stock_prices` | ~84K daily OHLCV prices with `daily_return_pct` |
# MAGIC | `fact_sp500_benchmark` | ~1,254 daily S&P 500 index prices for benchmarking |
# MAGIC | `fact_financials` | ~319 annual financials (income statement + balance sheet + cash flow) with 28 computed metrics |
# MAGIC
# MAGIC **Runtime:** ~15-20 minutes (sequential API calls per ticker)
# MAGIC
# MAGIC ### Key design decisions:
# MAGIC - **Dividend yield** is computed as `dividendRate / currentPrice × 100` (not yfinance's inconsistent `dividendYield` field)
# MAGIC - **Financial statements** come directly from yfinance (income, balance sheet, cash flow) — no marketplace datasets needed
# MAGIC - Derived ratios include: gross/operating/net margin, ROA, ROE, debt-to-asset, current ratio, revenue growth YoY

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's look at the company data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Company profiles: sector, market cap, dividends, valuations
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     company_name,
# MAGIC     sector,
# MAGIC     ROUND(market_cap / 1e9, 1) AS market_cap_B,
# MAGIC     ROUND(dividend_yield, 2) AS div_yield_pct,
# MAGIC     ROUND(trailing_pe, 1) AS pe,
# MAGIC     ROUND(beta, 2) AS beta
# MAGIC FROM dim_companies
# MAGIC ORDER BY market_cap DESC
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sector distribution
# MAGIC SELECT
# MAGIC     sector,
# MAGIC     COUNT(*) AS companies,
# MAGIC     ROUND(AVG(market_cap) / 1e9, 1) AS avg_mktcap_B,
# MAGIC     ROUND(AVG(CASE WHEN dividend_yield > 0 THEN dividend_yield END), 2) AS avg_div_yield,
# MAGIC     ROUND(AVG(trailing_pe), 1) AS avg_pe,
# MAGIC     ROUND(AVG(beta), 2) AS avg_beta
# MAGIC FROM dim_companies
# MAGIC GROUP BY sector
# MAGIC ORDER BY companies DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Stock price data coverage

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Date range and ticker count
# MAGIC SELECT
# MAGIC     MIN(trade_date) AS earliest,
# MAGIC     MAX(trade_date) AS latest,
# MAGIC     COUNT(DISTINCT ticker) AS tickers,
# MAGIC     COUNT(*) AS total_rows,
# MAGIC     ROUND(COUNT(*) / COUNT(DISTINCT ticker), 0) AS avg_days_per_ticker
# MAGIC FROM fact_stock_prices

# COMMAND ----------

# MAGIC %md
# MAGIC ### S&P 500 benchmark

# COMMAND ----------

# MAGIC %sql
# MAGIC -- S&P 500 index data for relative performance analysis
# MAGIC SELECT
# MAGIC     MIN(trade_date) AS earliest,
# MAGIC     MAX(trade_date) AS latest,
# MAGIC     COUNT(*) AS trading_days,
# MAGIC     ROUND(MIN(close_price), 2) AS min_close,
# MAGIC     ROUND(MAX(close_price), 2) AS max_close
# MAGIC FROM fact_sp500_benchmark

# COMMAND ----------

# MAGIC %md
# MAGIC ### Annual financials

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Financials coverage by year
# MAGIC SELECT
# MAGIC     fiscal_year,
# MAGIC     COUNT(DISTINCT ticker) AS companies,
# MAGIC     ROUND(SUM(revenue) / 1e9, 1) AS total_revenue_B,
# MAGIC     ROUND(AVG(net_margin_pct), 1) AS avg_net_margin
# MAGIC FROM fact_financials
# MAGIC WHERE revenue IS NOT NULL
# MAGIC GROUP BY fiscal_year
# MAGIC ORDER BY fiscal_year

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Economic Data (IMF WEO via Volume)
# MAGIC
# MAGIC **`03_load_economic_data.py`** reads a pre-uploaded TSV file from a Unity Catalog Volume:
# MAGIC
# MAGIC | Aspect | Detail |
# MAGIC |--------|--------|
# MAGIC | **Source** | IMF World Economic Outlook, October 2024 release |
# MAGIC | **File** | `WEOOct2024all_utf8.tsv` uploaded to `/Volumes/{catalog}/{schema}/imf_data/` |
# MAGIC | **Countries** | 20 major economies (US, UK, China, Japan, Germany, France, India, Canada, Australia, Brazil, Mexico, Italy, Spain, South Korea, Switzerland, Netherlands, Sweden, Singapore, Hong Kong SAR, Indonesia) |
# MAGIC | **Indicators** | GDP Growth Rate, Inflation Rate, Unemployment Rate, Government Debt (% of GDP), Current Account Balance (% of GDP) |
# MAGIC | **Years** | 2015–2029 (actuals + IMF projections, flagged via `is_projection`) |
# MAGIC | **Runtime** | Under 1 minute (local Volume read, no network) |

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Economic data coverage
# MAGIC SELECT
# MAGIC     indicator_name,
# MAGIC     COUNT(DISTINCT country) AS countries,
# MAGIC     MIN(year) AS from_year,
# MAGIC     MAX(year) AS to_year,
# MAGIC     SUM(CASE WHEN is_projection = 'N' THEN 1 ELSE 0 END) AS actuals,
# MAGIC     SUM(CASE WHEN is_projection = 'Y' THEN 1 ELSE 0 END) AS projections
# MAGIC FROM fact_economic_indicators
# MAGIC GROUP BY indicator_name
# MAGIC ORDER BY indicator_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: GDP growth — US vs China
# MAGIC SELECT country, year, value AS gdp_growth_pct, is_projection
# MAGIC FROM fact_economic_indicators
# MAGIC WHERE indicator_name = 'GDP Growth Rate (%)'
# MAGIC   AND country IN ('United States', 'China')
# MAGIC ORDER BY country, year

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Data Model — 4 Pre-Joined Views
# MAGIC
# MAGIC **`04_create_data_model.sql`** creates the views that Genie uses as data sources.
# MAGIC
# MAGIC | View | Description | Row Grain |
# MAGIC |------|-------------|-----------|
# MAGIC | **vw_company_fundamentals** | Company profiles + annual financials + dividends + P/E + P/B + beta | One row per company per fiscal year |
# MAGIC | **vw_stock_performance** | Company data + daily stock prices | One row per company per trading day |
# MAGIC | **vw_stock_vs_benchmark** | Stock prices + S&P 500 index with `excess_return_pct` | One row per company per trading day |
# MAGIC | **vw_economic_overview** | IMF macroeconomic indicators with `is_projection` flag | One row per country per indicator per year |
# MAGIC
# MAGIC ### Why views instead of raw tables for Genie?
# MAGIC - **Simpler** — Genie doesn't need to infer join logic
# MAGIC - **Fewer errors** — Pre-joined data eliminates incorrect joins
# MAGIC - **Consistent metrics** — Computed columns like `excess_return_pct` and `net_margin_pct` are defined once

# COMMAND ----------

# MAGIC %md
# MAGIC ### vw_company_fundamentals
# MAGIC Company profiles joined with annual financials, dividends, and valuation multiples.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 companies by revenue (most recent year)
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     company_name,
# MAGIC     sector,
# MAGIC     fiscal_year,
# MAGIC     ROUND(revenue / 1e9, 1) AS revenue_B,
# MAGIC     ROUND(net_margin_pct, 1) AS net_margin_pct,
# MAGIC     ROUND(dividend_yield, 2) AS div_yield_pct,
# MAGIC     ROUND(trailing_pe, 1) AS pe,
# MAGIC     ROUND(beta, 2) AS beta
# MAGIC FROM vw_company_fundamentals
# MAGIC WHERE fiscal_year = (SELECT MAX(fiscal_year) FROM vw_company_fundamentals)
# MAGIC ORDER BY revenue DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### vw_stock_performance
# MAGIC Company data joined with daily stock prices.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Apple stock: last 10 trading days
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     trade_date,
# MAGIC     ROUND(close_price, 2) AS close,
# MAGIC     volume,
# MAGIC     ROUND(daily_return_pct, 2) AS return_pct
# MAGIC FROM vw_stock_performance
# MAGIC WHERE ticker = 'AAPL'
# MAGIC ORDER BY trade_date DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### vw_stock_vs_benchmark
# MAGIC Stock prices joined with S&P 500 index data — includes `excess_return_pct`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Apple vs S&P 500: last 10 trading days
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     trade_date,
# MAGIC     ROUND(close_price, 2) AS stock_close,
# MAGIC     ROUND(sp500_close, 2) AS sp500_close,
# MAGIC     ROUND(stock_daily_return_pct, 2) AS stock_return,
# MAGIC     ROUND(sp500_daily_return_pct, 2) AS sp500_return,
# MAGIC     ROUND(excess_return_pct, 2) AS excess_return
# MAGIC FROM vw_stock_vs_benchmark
# MAGIC WHERE ticker = 'AAPL'
# MAGIC ORDER BY trade_date DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### vw_economic_overview
# MAGIC IMF macroeconomic indicators — actuals and projections flagged.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inflation comparison across major economies (latest actual year)
# MAGIC SELECT
# MAGIC     country,
# MAGIC     year,
# MAGIC     ROUND(value, 2) AS inflation_pct,
# MAGIC     is_projection
# MAGIC FROM vw_economic_overview
# MAGIC WHERE indicator_name = 'Inflation Rate (%)'
# MAGIC   AND year = (
# MAGIC     SELECT MAX(year) FROM vw_economic_overview
# MAGIC     WHERE indicator_name = 'Inflation Rate (%)' AND is_projection = 'N'
# MAGIC   )
# MAGIC ORDER BY value DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Grant Access
# MAGIC
# MAGIC **`05_grant_genie_access.py`** grants `SELECT` on all 10 objects to the consumer.
# MAGIC
# MAGIC - If no consumer is specified, it **auto-detects the current user** via `SELECT current_user()`
# MAGIC - Also grants `USE CATALOG` and `USE SCHEMA`
# MAGIC - Runs in under 1 minute

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Data Validation
# MAGIC
# MAGIC Let's verify everything is in place.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Full row count inventory
# MAGIC SELECT 'dim_companies' AS object, COUNT(*) AS rows FROM dim_companies
# MAGIC UNION ALL SELECT 'dim_sectors', COUNT(*) FROM dim_sectors
# MAGIC UNION ALL SELECT 'fact_stock_prices', COUNT(*) FROM fact_stock_prices
# MAGIC UNION ALL SELECT 'fact_sp500_benchmark', COUNT(*) FROM fact_sp500_benchmark
# MAGIC UNION ALL SELECT 'fact_financials', COUNT(*) FROM fact_financials
# MAGIC UNION ALL SELECT 'fact_economic_indicators', COUNT(*) FROM fact_economic_indicators
# MAGIC UNION ALL SELECT 'vw_company_fundamentals', COUNT(*) FROM vw_company_fundamentals
# MAGIC UNION ALL SELECT 'vw_stock_performance', COUNT(*) FROM vw_stock_performance
# MAGIC UNION ALL SELECT 'vw_stock_vs_benchmark', COUNT(*) FROM vw_stock_vs_benchmark
# MAGIC UNION ALL SELECT 'vw_economic_overview', COUNT(*) FROM vw_economic_overview

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dividend coverage by sector

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     sector,
# MAGIC     COUNT(*) AS total,
# MAGIC     SUM(CASE WHEN dividend_yield > 0 THEN 1 ELSE 0 END) AS payers,
# MAGIC     ROUND(AVG(CASE WHEN dividend_yield > 0 THEN dividend_yield END), 2) AS avg_yield_pct
# MAGIC FROM dim_companies
# MAGIC GROUP BY sector
# MAGIC ORDER BY avg_yield_pct DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Valuation spread by sector

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     sector,
# MAGIC     ROUND(AVG(trailing_pe), 1) AS avg_pe,
# MAGIC     ROUND(AVG(price_to_book), 1) AS avg_pb,
# MAGIC     ROUND(AVG(beta), 2) AS avg_beta
# MAGIC FROM dim_companies
# MAGIC WHERE trailing_pe > 0
# MAGIC GROUP BY sector
# MAGIC ORDER BY avg_pe DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Sample Questions This Data Enables
# MAGIC
# MAGIC Once a Genie space is configured (Module 4), users can ask questions like:
# MAGIC
# MAGIC ### Company financials and valuations
# MAGIC - *"Which tech companies have the highest revenue growth?"*
# MAGIC - *"Show me companies with dividend yield above 3%"*
# MAGIC - *"Which stocks have the lowest P/E ratio in the Financials sector?"*
# MAGIC - *"Compare Apple and Microsoft's profit margins over the last 5 years"*
# MAGIC
# MAGIC ### Stock performance and benchmarking
# MAGIC - *"How did Tesla perform compared to the S&P 500 last year?"*
# MAGIC - *"Which sector had the best returns in 2024?"*
# MAGIC - *"What are the top 5 most volatile stocks by beta?"*
# MAGIC - *"Show me the highest volume trading days for NVIDIA"*
# MAGIC
# MAGIC ### Macroeconomic context
# MAGIC - *"What is the GDP growth forecast for India in 2026?"*
# MAGIC - *"Compare inflation rates between the US, UK, and Germany"*
# MAGIC - *"Which country has the highest government debt as a percentage of GDP?"*
# MAGIC - *"Show me unemployment trends in Japan over the last 10 years"*

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Let's try a few of those queries ourselves

# COMMAND ----------

# MAGIC %sql
# MAGIC -- High dividend yield stocks
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     company_name,
# MAGIC     sector,
# MAGIC     ROUND(dividend_yield, 2) AS div_yield_pct,
# MAGIC     ROUND(dividend_rate, 2) AS annual_div_usd,
# MAGIC     ROUND(payout_ratio, 1) AS payout_ratio_pct
# MAGIC FROM vw_company_fundamentals
# MAGIC WHERE dividend_yield > 0
# MAGIC   AND fiscal_year = (SELECT MAX(fiscal_year) FROM vw_company_fundamentals)
# MAGIC ORDER BY dividend_yield DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Undervalued stocks: low P/E, profitable
# MAGIC SELECT
# MAGIC     ticker,
# MAGIC     company_name,
# MAGIC     sector,
# MAGIC     ROUND(trailing_pe, 1) AS pe,
# MAGIC     ROUND(forward_pe, 1) AS fwd_pe,
# MAGIC     ROUND(price_to_book, 1) AS pb,
# MAGIC     ROUND(net_margin_pct, 1) AS margin_pct
# MAGIC FROM vw_company_fundamentals
# MAGIC WHERE trailing_pe > 0 AND trailing_pe < 15
# MAGIC   AND fiscal_year = (SELECT MAX(fiscal_year) FROM vw_company_fundamentals)
# MAGIC ORDER BY trailing_pe

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Which sectors outperformed the S&P 500 this year?
# MAGIC SELECT
# MAGIC     sector,
# MAGIC     ROUND(AVG(stock_daily_return_pct), 4) AS avg_sector_return,
# MAGIC     ROUND(AVG(sp500_daily_return_pct), 4) AS avg_sp500_return,
# MAGIC     ROUND(AVG(excess_return_pct), 4) AS avg_excess_return,
# MAGIC     COUNT(DISTINCT ticker) AS stocks
# MAGIC FROM vw_stock_vs_benchmark
# MAGIC WHERE trade_year = YEAR(CURRENT_DATE())
# MAGIC GROUP BY sector
# MAGIC ORDER BY avg_excess_return DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Government debt comparison across G7 (latest actual year)
# MAGIC SELECT
# MAGIC     country,
# MAGIC     year,
# MAGIC     ROUND(value, 1) AS govt_debt_pct_gdp,
# MAGIC     is_projection
# MAGIC FROM vw_economic_overview
# MAGIC WHERE indicator_name = 'Government Debt (% of GDP)'
# MAGIC   AND country IN ('United States', 'Japan', 'Germany', 'United Kingdom', 'France', 'Italy', 'Canada')
# MAGIC   AND year = (
# MAGIC     SELECT MAX(year) FROM vw_economic_overview
# MAGIC     WHERE indicator_name = 'Government Debt (% of GDP)' AND is_projection = 'N'
# MAGIC   )
# MAGIC ORDER BY value DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Two ingestion patterns:** REST API (yfinance) and pre-uploaded file via Volume (IMF WEO)
# MAGIC 2. **4 pre-joined views** simplify the schema for Genie — no raw tables exposed
# MAGIC 3. **Dividends, valuations, and beta** enable investment screening questions
# MAGIC 4. **S&P 500 benchmark** enables relative performance analysis via `excess_return_pct`
# MAGIC 5. **IMF projections** are flagged so Genie distinguishes actuals from forecasts
# MAGIC 6. **Unity Catalog metadata** (comments on tables and columns) directly improves Genie accuracy
# MAGIC 7. **Serverless compute** — no cluster configuration needed
# MAGIC 8. **`00_setup_prerequisites.py`** handles all prerequisites (catalog, schema, Volume, file upload) in one idempotent run
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Next:** Module 4 — Create the Genie space with these views
