# Databricks notebook source

# MAGIC %md
# MAGIC # Load Stock Price Data via yfinance
# MAGIC
# MAGIC This notebook pulls daily stock price data for S&P 500 companies and loads it into
# MAGIC Unity Catalog tables. It also creates a company dimension table with sector and
# MAGIC industry classifications.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - `yfinance` library installed on the cluster (`%pip install yfinance`)
# MAGIC - A catalog and schema created for the training (e.g., `genie_training.genie_demo`)
# MAGIC - Unity Catalog enabled on the workspace

# COMMAND ----------

# MAGIC %pip install yfinance --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType, DateType
)
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC These values can be overridden via Databricks Asset Bundle parameters
# MAGIC or by editing the defaults below.

# COMMAND ----------

dbutils.widgets.text("catalog", "training")
dbutils.widgets.text("schema", "genie")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
PRICE_HISTORY_YEARS = 5

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Target: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Define S&P 500 Universe
# MAGIC
# MAGIC We use a curated list of well-known, high-profile companies across all 11 GICS
# MAGIC sectors. This keeps the dataset manageable while covering the breadth an investment
# MAGIC audience expects.

# COMMAND ----------

SP500_SAMPLE = {
    "Technology": {
        "AAPL": "Apple Inc.",
        "MSFT": "Microsoft Corporation",
        "GOOGL": "Alphabet Inc.",
        "NVDA": "NVIDIA Corporation",
        "META": "Meta Platforms Inc.",
        "AVGO": "Broadcom Inc.",
        "CRM": "Salesforce Inc.",
        "ADBE": "Adobe Inc.",
        "AMD": "Advanced Micro Devices",
        "INTC": "Intel Corporation",
    },
    "Healthcare": {
        "JNJ": "Johnson & Johnson",
        "UNH": "UnitedHealth Group",
        "PFE": "Pfizer Inc.",
        "ABBV": "AbbVie Inc.",
        "LLY": "Eli Lilly and Company",
        "MRK": "Merck & Co.",
        "TMO": "Thermo Fisher Scientific",
        "ABT": "Abbott Laboratories",
    },
    "Financials": {
        "JPM": "JPMorgan Chase & Co.",
        "BAC": "Bank of America Corp.",
        "WFC": "Wells Fargo & Company",
        "GS": "Goldman Sachs Group",
        "MS": "Morgan Stanley",
        "BLK": "BlackRock Inc.",
        "AXP": "American Express Co.",
        "C": "Citigroup Inc.",
    },
    "Consumer Discretionary": {
        "AMZN": "Amazon.com Inc.",
        "TSLA": "Tesla Inc.",
        "HD": "The Home Depot",
        "MCD": "McDonald's Corporation",
        "NKE": "Nike Inc.",
        "SBUX": "Starbucks Corporation",
        "TGT": "Target Corporation",
    },
    "Consumer Staples": {
        "PG": "Procter & Gamble Co.",
        "KO": "The Coca-Cola Company",
        "PEP": "PepsiCo Inc.",
        "COST": "Costco Wholesale Corp.",
        "WMT": "Walmart Inc.",
        "CL": "Colgate-Palmolive Co.",
    },
    "Energy": {
        "XOM": "Exxon Mobil Corporation",
        "CVX": "Chevron Corporation",
        "COP": "ConocoPhillips",
        "SLB": "Schlumberger Limited",
        "EOG": "EOG Resources Inc.",
    },
    "Industrials": {
        "CAT": "Caterpillar Inc.",
        "BA": "The Boeing Company",
        "HON": "Honeywell International",
        "UPS": "United Parcel Service",
        "GE": "GE Aerospace",
        "RTX": "RTX Corporation",
    },
    "Materials": {
        "LIN": "Linde plc",
        "APD": "Air Products & Chemicals",
        "SHW": "Sherwin-Williams Co.",
        "FCX": "Freeport-McMoRan Inc.",
    },
    "Real Estate": {
        "AMT": "American Tower Corp.",
        "PLD": "Prologis Inc.",
        "CCI": "Crown Castle Inc.",
        "SPG": "Simon Property Group",
    },
    "Utilities": {
        "NEE": "NextEra Energy Inc.",
        "DUK": "Duke Energy Corp.",
        "SO": "Southern Company",
        "D": "Dominion Energy Inc.",
    },
    "Communication Services": {
        "DIS": "The Walt Disney Company",
        "NFLX": "Netflix Inc.",
        "CMCSA": "Comcast Corporation",
        "T": "AT&T Inc.",
        "VZ": "Verizon Communications",
    },
}

companies_list = []
for sector, tickers in SP500_SAMPLE.items():
    for ticker, name in tickers.items():
        companies_list.append({
            "ticker": ticker,
            "company_name": name,
            "sector": sector,
        })

companies_pdf = pd.DataFrame(companies_list)
print(f"Total companies: {len(companies_pdf)}")
companies_pdf.head(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Fetch Company Metadata from yfinance
# MAGIC
# MAGIC Enrich the company list with industry, country, market cap, and employee count.

# COMMAND ----------

enriched = []
errors = []

for _, row in companies_pdf.iterrows():
    ticker = row["ticker"]
    try:
        info = yf.Ticker(ticker).info

        div_rate = float(info.get("dividendRate", 0) or 0)
        cur_price = float(
            info.get("currentPrice", 0)
            or info.get("regularMarketPrice", 0)
            or 0
        )
        div_yield = round(div_rate / cur_price * 100, 2) if cur_price > 0 else 0.0

        raw_payout = float(info.get("payoutRatio", 0) or 0)
        payout = round(raw_payout * 100, 2) if raw_payout <= 5 else round(raw_payout, 2)

        enriched.append({
            "ticker": ticker,
            "company_name": row["company_name"],
            "sector": info.get("sector", row["sector"]),
            "industry": info.get("industry", "Unknown"),
            "headquarters_country": info.get("country", "United States"),
            "market_cap": float(info.get("marketCap", 0)),
            "employees": int(info.get("fullTimeEmployees", 0)),
            "exchange": info.get("exchange", "Unknown"),
            "currency": info.get("currency", "USD"),
            "dividend_yield": div_yield,
            "dividend_rate": div_rate,
            "trailing_pe": float(info.get("trailingPE", 0) or 0),
            "forward_pe": float(info.get("forwardPE", 0) or 0),
            "price_to_book": float(info.get("priceToBook", 0) or 0),
            "payout_ratio": payout,
            "beta": float(info.get("beta", 0) or 0),
            "fifty_two_week_high": float(info.get("fiftyTwoWeekHigh", 0) or 0),
            "fifty_two_week_low": float(info.get("fiftyTwoWeekLow", 0) or 0),
        })
    except Exception as e:
        errors.append({"ticker": ticker, "error": str(e)})
        enriched.append({
            "ticker": ticker,
            "company_name": row["company_name"],
            "sector": row["sector"],
            "industry": "Unknown",
            "headquarters_country": "United States",
            "market_cap": 0.0,
            "employees": 0,
            "exchange": "Unknown",
            "currency": "USD",
            "dividend_yield": 0.0,
            "dividend_rate": 0.0,
            "trailing_pe": 0.0,
            "forward_pe": 0.0,
            "price_to_book": 0.0,
            "payout_ratio": 0.0,
            "beta": 0.0,
            "fifty_two_week_high": 0.0,
            "fifty_two_week_low": 0.0,
        })

if errors:
    print(f"Warnings: {len(errors)} tickers had issues fetching metadata:")
    for e in errors[:5]:
        print(f"  {e['ticker']}: {e['error']}")

enriched_pdf = pd.DataFrame(enriched)
print(f"\nEnriched {len(enriched_pdf)} companies")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create dim_companies Table

# COMMAND ----------

dim_companies_df = spark.createDataFrame(enriched_pdf)

dim_companies_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.{SCHEMA}.dim_companies")

print(f"Created {CATALOG}.{SCHEMA}.dim_companies")
display(spark.table(f"{CATALOG}.{SCHEMA}.dim_companies").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create dim_sectors Table

# COMMAND ----------

sectors_data = [
    ("Technology", "Companies in software, hardware, semiconductors, and IT services"),
    ("Healthcare", "Pharmaceuticals, biotechnology, medical devices, and health services"),
    ("Financials", "Banks, investment firms, insurance, and financial services"),
    ("Consumer Discretionary", "Retail, automotive, apparel, and entertainment"),
    ("Consumer Staples", "Food, beverage, household products, and personal care"),
    ("Energy", "Oil & gas exploration, production, refining, and renewable energy"),
    ("Industrials", "Aerospace, defense, machinery, transportation, and construction"),
    ("Materials", "Chemicals, metals, mining, paper, and construction materials"),
    ("Real Estate", "REITs, property management, and real estate development"),
    ("Utilities", "Electric, gas, water utilities, and renewable power generation"),
    ("Communication Services", "Media, entertainment, telecom, and interactive services"),
]

sectors_schema = StructType([
    StructField("sector_name", StringType(), False),
    StructField("description", StringType(), True),
])

dim_sectors_df = spark.createDataFrame(sectors_data, schema=sectors_schema)
dim_sectors_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.dim_sectors")

print(f"Created {CATALOG}.{SCHEMA}.dim_sectors")
display(spark.table(f"{CATALOG}.{SCHEMA}.dim_sectors"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Fetch Historical Stock Prices
# MAGIC
# MAGIC Pull daily OHLCV data for each company for the last N years.

# COMMAND ----------

end_date = datetime.today().strftime("%Y-%m-%d")
start_date = (datetime.today() - timedelta(days=365 * PRICE_HISTORY_YEARS)).strftime("%Y-%m-%d")

print(f"Fetching price data from {start_date} to {end_date}")

all_prices = []
price_errors = []

tickers_list = enriched_pdf["ticker"].tolist()

for ticker in tickers_list:
    try:
        hist = yf.download(ticker, start=start_date, end=end_date, progress=False)
        if hist.empty:
            price_errors.append(ticker)
            continue
        hist = hist.reset_index()
        hist.columns = hist.columns.droplevel(1) if isinstance(hist.columns, pd.MultiIndex) else hist.columns
        hist["ticker"] = ticker
        hist = hist.rename(columns={
            "Date": "trade_date",
            "Open": "open_price",
            "High": "high_price",
            "Low": "low_price",
            "Close": "close_price",
            "Volume": "volume",
        })
        hist["trade_date"] = pd.to_datetime(hist["trade_date"]).dt.date
        hist["daily_return_pct"] = hist["close_price"].pct_change() * 100
        hist = hist[["ticker", "trade_date", "open_price", "high_price",
                      "low_price", "close_price", "volume", "daily_return_pct"]]
        all_prices.append(hist)
    except Exception as e:
        price_errors.append(ticker)
        print(f"  Error fetching {ticker}: {e}")

if price_errors:
    print(f"\nSkipped {len(price_errors)} tickers due to errors: {price_errors[:10]}")

prices_pdf = pd.concat(all_prices, ignore_index=True)
prices_pdf["daily_return_pct"] = prices_pdf["daily_return_pct"].fillna(0.0)
print(f"\nTotal price records: {len(prices_pdf):,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Create fact_stock_prices Table

# COMMAND ----------

fact_prices_df = spark.createDataFrame(prices_pdf)

fact_prices_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.fact_stock_prices")

print(f"Created {CATALOG}.{SCHEMA}.fact_stock_prices")
print(f"  Records: {spark.table(f'{CATALOG}.{SCHEMA}.fact_stock_prices').count():,}")
display(spark.table(f"{CATALOG}.{SCHEMA}.fact_stock_prices").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Add Unity Catalog Table and Column Comments
# MAGIC
# MAGIC Well-annotated metadata is critical for Genie accuracy.

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA}.dim_companies
    SET TBLPROPERTIES ('comment' = 'Dimension table of S&P 500 companies with sector, industry, and market capitalization. One row per company.')
""")

column_comments_companies = {
    "ticker": "Stock ticker symbol (e.g., AAPL, MSFT). Primary key.",
    "company_name": "Full legal company name",
    "sector": "GICS sector classification (e.g., Technology, Healthcare, Financials)",
    "industry": "GICS industry sub-classification within the sector",
    "headquarters_country": "Country where the company is headquartered",
    "market_cap": "Market capitalization in USD. Total value of outstanding shares.",
    "employees": "Number of full-time employees",
    "exchange": "Stock exchange where the company is listed (e.g., NMS, NYQ)",
    "currency": "Currency used for financial reporting",
    "dividend_yield": "Annual dividend yield as a percentage of current stock price. 0 means no dividend.",
    "dividend_rate": "Annual dividend payment per share in USD",
    "trailing_pe": "Trailing price-to-earnings ratio (stock price / last 12 months EPS). Key valuation metric.",
    "forward_pe": "Forward price-to-earnings ratio (stock price / estimated next 12 months EPS)",
    "price_to_book": "Price-to-book ratio (stock price / book value per share). Values below 1 may indicate undervaluation.",
    "payout_ratio": "Percentage of earnings paid out as dividends. High ratios may indicate limited reinvestment.",
    "beta": "Stock beta relative to the market. Values > 1 indicate higher volatility than the S&P 500.",
    "fifty_two_week_high": "Highest stock price in the last 52 weeks in USD",
    "fifty_two_week_low": "Lowest stock price in the last 52 weeks in USD",
}

for col, comment in column_comments_companies.items():
    spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.dim_companies ALTER COLUMN {col} COMMENT '{comment}'")

spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA}.dim_sectors
    SET TBLPROPERTIES ('comment' = 'Reference table of GICS sector classifications with descriptions.')
""")

spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA}.fact_stock_prices
    SET TBLPROPERTIES ('comment' = 'Daily stock price data (OHLCV) for S&P 500 companies. One row per ticker per trading day.')
""")

column_comments_prices = {
    "ticker": "Stock ticker symbol. Foreign key to dim_companies.",
    "trade_date": "The trading date for this price record",
    "open_price": "Opening price in USD at market open",
    "high_price": "Highest price in USD reached during the trading day",
    "low_price": "Lowest price in USD reached during the trading day",
    "close_price": "Closing price in USD at market close. Use this for return calculations.",
    "volume": "Number of shares traded during the day",
    "daily_return_pct": "Daily percentage return calculated as (close - previous_close) / previous_close * 100",
}

for col, comment in column_comments_prices.items():
    spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.fact_stock_prices ALTER COLUMN {col} COMMENT '{comment}'")

print("Table and column comments applied successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Load S&P 500 Benchmark Index
# MAGIC
# MAGIC Pull daily price data for the S&P 500 index (`^GSPC`) so users can compare
# MAGIC individual stock performance against the market benchmark.

# COMMAND ----------

print("Fetching S&P 500 index data...")
try:
    sp500_hist = yf.download("^GSPC", start=start_date, end=end_date, progress=False)
    sp500_hist = sp500_hist.reset_index()
    sp500_hist.columns = sp500_hist.columns.droplevel(1) if isinstance(sp500_hist.columns, pd.MultiIndex) else sp500_hist.columns
    sp500_hist = sp500_hist.rename(columns={
        "Date": "trade_date",
        "Open": "open_price",
        "High": "high_price",
        "Low": "low_price",
        "Close": "close_price",
        "Volume": "volume",
    })
    sp500_hist["trade_date"] = pd.to_datetime(sp500_hist["trade_date"]).dt.date
    sp500_hist["daily_return_pct"] = sp500_hist["close_price"].pct_change() * 100
    sp500_hist["daily_return_pct"] = sp500_hist["daily_return_pct"].fillna(0.0)
    sp500_hist = sp500_hist[["trade_date", "open_price", "high_price",
                              "low_price", "close_price", "volume", "daily_return_pct"]]

    sp500_df = spark.createDataFrame(sp500_hist)
    sp500_df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.fact_sp500_benchmark")

    spark.sql(f"""
        ALTER TABLE {CATALOG}.{SCHEMA}.fact_sp500_benchmark
        SET TBLPROPERTIES ('comment' = 'Daily S&P 500 index price data (OHLCV). Use to benchmark individual stock performance against the overall US market. One row per trading day.')
    """)

    sp500_column_comments = {
        "trade_date": "Trading date",
        "open_price": "S&P 500 index opening value",
        "high_price": "S&P 500 index highest value during the day",
        "low_price": "S&P 500 index lowest value during the day",
        "close_price": "S&P 500 index closing value. Use for return calculations.",
        "volume": "Total shares traded across all S&P 500 constituents",
        "daily_return_pct": "Daily percentage return of the S&P 500 index",
    }
    for col, comment in sp500_column_comments.items():
        spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.fact_sp500_benchmark ALTER COLUMN {col} COMMENT '{comment}'")

    print(f"Created {CATALOG}.{SCHEMA}.fact_sp500_benchmark with {sp500_df.count():,} rows")
except Exception as e:
    print(f"Warning: Could not fetch S&P 500 data: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Fetch Annual Financial Statements
# MAGIC
# MAGIC Pull income statements, balance sheets, and cash-flow statements from yfinance
# MAGIC for every company. This replaces the EDGAR Marketplace sample (which only
# MAGIC covered a handful of tickers).

# COMMAND ----------

print("Fetching annual financial statements from yfinance...")

fin_records = []
fin_errors = []

def _safe(df, field, col):
    """Extract a value from a financial-statement DataFrame, returning None on miss."""
    try:
        v = df.loc[field, col]
        return float(v) if pd.notna(v) else None
    except (KeyError, ValueError, TypeError):
        return None

for ticker in tickers_list:
    try:
        t = yf.Ticker(ticker)
        inc = t.income_stmt         # annual income statement
        bs  = t.balance_sheet       # annual balance sheet
        cf  = t.cash_flow           # annual cash-flow statement

        if inc is None or inc.empty:
            fin_errors.append(ticker)
            continue

        for col in inc.columns:
            fiscal_year = col.year

            revenue        = _safe(inc, "Total Revenue", col)
            cost_of_rev    = _safe(inc, "Cost Of Revenue", col)
            gross_profit   = _safe(inc, "Gross Profit", col)
            operating_inc  = _safe(inc, "Operating Income", col) or _safe(inc, "EBIT", col)
            net_income     = _safe(inc, "Net Income", col)
            eps            = _safe(inc, "Diluted EPS", col) or _safe(inc, "Basic EPS", col)

            total_assets   = _safe(bs, "Total Assets", col)
            total_liab     = _safe(bs, "Total Liabilities Net Minority Interest", col)
            equity         = _safe(bs, "Stockholders Equity", col)
            cash           = _safe(bs, "Cash And Cash Equivalents", col)
            lt_debt        = _safe(bs, "Long Term Debt", col)
            curr_assets    = _safe(bs, "Current Assets", col)
            curr_liab      = _safe(bs, "Current Liabilities", col)

            op_cf          = _safe(cf, "Operating Cash Flow", col)

            def _pct(num, den):
                if num is not None and den is not None and den != 0:
                    return round(num / den * 100, 2)
                return None

            def _ratio(num, den):
                if num is not None and den is not None and den != 0:
                    return round(num / den, 2)
                return None

            fin_records.append({
                "ticker": ticker,
                "fiscal_year": fiscal_year,
                "revenue": revenue,
                "net_income": net_income,
                "total_assets": total_assets,
                "total_liabilities": total_liab,
                "stockholders_equity": equity,
                "eps": eps,
                "gross_profit": gross_profit,
                "operating_income": operating_inc,
                "operating_cash_flow": op_cf,
                "cost_of_revenue": cost_of_rev,
                "cash": cash,
                "long_term_debt": lt_debt,
                "current_assets": curr_assets,
                "current_liabilities": curr_liab,
                "gross_margin_pct": _pct(gross_profit, revenue),
                "operating_margin_pct": _pct(operating_inc, revenue),
                "net_margin_pct": _pct(net_income, revenue),
                "return_on_assets_pct": _pct(net_income, total_assets),
                "return_on_equity_pct": _pct(net_income, equity),
                "current_ratio": _ratio(curr_assets, curr_liab),
                "debt_to_equity": _ratio(lt_debt, equity),
                "debt_ratio_pct": _pct(total_liab, total_assets),
                "cash_flow_margin_pct": _pct(op_cf, revenue),
                "debt_to_asset_ratio": _ratio(total_liab, total_assets),
            })
    except Exception as e:
        fin_errors.append(ticker)
        print(f"  {ticker}: {e}")

if fin_errors:
    print(f"Warnings: {len(fin_errors)} tickers had issues: {fin_errors[:10]}")

fin_pdf = pd.DataFrame(fin_records)

# Revenue / net-income growth (YoY) per ticker
fin_pdf = fin_pdf.sort_values(["ticker", "fiscal_year"])
fin_pdf["revenue_growth_pct"] = (
    fin_pdf.groupby("ticker")["revenue"]
    .pct_change() * 100
).round(2)
fin_pdf["net_income_growth_pct"] = (
    fin_pdf.groupby("ticker")["net_income"]
    .pct_change() * 100
).round(2)

print(f"\nTotal financial records: {len(fin_pdf):,}  ({fin_pdf['ticker'].nunique()} companies)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Create fact_financials Table

# COMMAND ----------

fin_df = spark.createDataFrame(fin_pdf)
fin_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.{SCHEMA}.fact_financials"
)

spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA}.fact_financials
    SET TBLPROPERTIES ('comment' = 'Annual financial statement data for S&P 500 companies sourced from yfinance (income statement, balance sheet, cash flow). One row per company per fiscal year. All monetary values in USD.')
""")

fin_col_comments = {
    "ticker": "Stock ticker symbol. Foreign key to dim_companies.",
    "fiscal_year": "Fiscal year of the financial filing (e.g., 2023, 2024)",
    "revenue": "Total revenue (net sales) in USD for the fiscal year",
    "net_income": "Net income (profit after taxes) in USD for the fiscal year",
    "total_assets": "Total assets on the balance sheet in USD",
    "total_liabilities": "Total liabilities on the balance sheet in USD",
    "stockholders_equity": "Total stockholders equity in USD",
    "eps": "Diluted earnings per share in USD",
    "gross_profit": "Gross profit in USD (revenue minus cost of revenue)",
    "operating_income": "Operating income (EBIT) in USD from core operations",
    "operating_cash_flow": "Cash generated from operating activities in USD",
    "cost_of_revenue": "Cost directly tied to revenue generation in USD",
    "cash": "Cash and cash equivalents in USD",
    "long_term_debt": "Long-term debt obligations beyond one year in USD",
    "current_assets": "Assets convertible within one year in USD",
    "current_liabilities": "Short-term obligations due within one year in USD",
    "gross_margin_pct": "Gross profit margin as a percentage of revenue",
    "operating_margin_pct": "Operating profit margin as a percentage of revenue",
    "net_margin_pct": "Net profit margin as a percentage of revenue",
    "return_on_assets_pct": "Return on assets (ROA) as a percentage",
    "return_on_equity_pct": "Return on equity (ROE) as a percentage",
    "current_ratio": "Current ratio: current assets / current liabilities. Above 1 = can cover short-term obligations.",
    "debt_to_equity": "Debt-to-equity ratio: long-term debt / equity",
    "debt_ratio_pct": "Debt ratio: total liabilities as a percentage of total assets",
    "cash_flow_margin_pct": "Operating cash flow as a percentage of revenue",
    "debt_to_asset_ratio": "Debt-to-asset ratio: total liabilities / total assets",
    "revenue_growth_pct": "Year-over-year revenue growth as a percentage",
    "net_income_growth_pct": "Year-over-year net income growth as a percentage",
}
for col, comment in fin_col_comments.items():
    spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.fact_financials ALTER COLUMN {col} COMMENT '{comment}'")

row_ct = spark.table(f"{CATALOG}.{SCHEMA}.fact_financials").count()
print(f"Created {CATALOG}.{SCHEMA}.fact_financials — {row_ct:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Validate the Data

# COMMAND ----------

print("=== Data Validation Summary ===\n")

for table in ["dim_companies", "dim_sectors", "fact_stock_prices", "fact_sp500_benchmark", "fact_financials"]:
    fqn = f"{CATALOG}.{SCHEMA}.{table}"
    count = spark.table(fqn).count()
    cols = len(spark.table(fqn).columns)
    print(f"  {table}: {count:,} rows, {cols} columns")

print("\n=== Companies by Sector ===")
display(
    spark.table(f"{CATALOG}.{SCHEMA}.dim_companies")
    .groupBy("sector")
    .count()
    .orderBy(F.desc("count"))
)

print("\n=== Dividend Yield by Sector ===")
display(
    spark.table(f"{CATALOG}.{SCHEMA}.dim_companies")
    .filter(F.col("dividend_yield") > 0)
    .groupBy("sector")
    .agg(
        F.round(F.avg("dividend_yield"), 2).alias("avg_dividend_yield_pct"),
        F.count("*").alias("dividend_payers"),
    )
    .orderBy(F.desc("avg_dividend_yield_pct"))
)

print("\n=== Price Data Date Range ===")
display(
    spark.table(f"{CATALOG}.{SCHEMA}.fact_stock_prices")
    .select(
        F.min("trade_date").alias("earliest_date"),
        F.max("trade_date").alias("latest_date"),
        F.countDistinct("ticker").alias("unique_tickers"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done!
# MAGIC
# MAGIC The following tables are now in Unity Catalog:
# MAGIC
# MAGIC | Table | Type | Description |
# MAGIC |-------|------|-------------|
# MAGIC | `dim_companies` | Dimension | Company profiles with sector, market cap, dividends, valuation multiples, and beta |
# MAGIC | `dim_sectors` | Dimension | GICS sector reference data |
# MAGIC | `fact_stock_prices` | Fact | Daily OHLCV stock prices with returns |
# MAGIC | `fact_sp500_benchmark` | Fact | Daily S&P 500 index data for market benchmarking |
# MAGIC | `fact_financials` | Fact | Annual income statement, balance sheet, and cash flow data from yfinance |
# MAGIC
# MAGIC **Next step:** Run `03_load_economic_data.py` to load IMF macroeconomic data,
# MAGIC then `04_create_data_model.sql` to create the Genie views.
