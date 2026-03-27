# Databricks notebook source
# MAGIC %md
# MAGIC # Step 6: Create Genie Space via Databricks SDK
# MAGIC
# MAGIC This notebook creates the **Investment Research Analyst** Genie space
# MAGIC programmatically using the Genie REST API.  It configures:
# MAGIC
# MAGIC - **4 views** as data sources (`vw_company_fundamentals`, `vw_stock_performance`, `vw_stock_vs_benchmark`, `vw_economic_overview`)
# MAGIC - **General instructions** covering data scope, guidelines, and domain terminology
# MAGIC - **10 example SQL queries** for common investment analysis questions
# MAGIC - **8 sample starter questions** shown when users first open the space
# MAGIC
# MAGIC > **Alternative:** The same Genie space can also be created via Terraform —
# MAGIC > see `00_infrastructure/` in this repository.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "training")
dbutils.widgets.text("schema", "genie")
dbutils.widgets.text("warehouse_id", "")
dbutils.widgets.text("space_title", "Investment Research Analyst_dab")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
SPACE_TITLE = dbutils.widgets.get("space_title")

print(f"Catalog:      {CATALOG}")
print(f"Schema:       {SCHEMA}")
print(f"Warehouse:    {WAREHOUSE_ID}")
print(f"Space title:  {SPACE_TITLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build the Genie space payload

# COMMAND ----------

import json, uuid

_counter = 0

def hex_id():
    """Generate a sorted 32-char hex ID (required by the Genie API)."""
    global _counter
    _counter += 1
    return f"{_counter:08x}{uuid.uuid4().hex[:24]}"

# COMMAND ----------

INSTRUCTIONS = f"""This Genie space is designed for investment research analysts and portfolio managers analyzing S&P 500 companies across financials, stock performance, and macroeconomic indicators.

DATA SCOPE:
- Company fundamentals: ~67 S&P 500 companies across all 11 GICS sectors with annual financials (income statement, balance sheet, cash flow), dividend yield, P/E, P/B, beta, and 52-week range
- Stock prices: daily OHLCV data for the last 5 years with daily return percentages
- S&P 500 benchmark: daily index data for relative performance analysis (excess_return_pct = stock return minus market return)
- Economic indicators: GDP growth, inflation, unemployment, government debt, and current account balance for 20 major economies (US, UK, China, Japan, Germany, France, India, Canada, Australia, Brazil, Mexico, Italy, Spain, South Korea, Switzerland, Netherlands, Sweden, Singapore, Hong Kong SAR, Indonesia). Includes both actuals and IMF projections (flagged via is_projection).

VIEWS:
- vw_company_fundamentals: company profiles + annual financials + dividends + valuation multiples. One row per company per fiscal year.
- vw_stock_performance: company data + daily stock prices. One row per company per trading day.
- vw_stock_vs_benchmark: stock prices + S&P 500 index data with excess_return_pct. Use for relative performance questions. One row per company per trading day.
- vw_economic_overview: IMF macroeconomic data. One row per country per indicator per year.

GUIDELINES:
- When users ask about company financials, always include fiscal_year to clarify the time period.
- When comparing companies, include both absolute values and percentage differences where applicable.
- For stock price analysis, prefer using daily_return_pct or calculated period returns over raw price levels when comparing across companies.
- For relative performance questions ("How did X outperform the market?"), use vw_stock_vs_benchmark and the excess_return_pct column.
- For dividend and valuation screening ("high yield stocks", "undervalued stocks"), use vw_company_fundamentals which includes dividend_yield, trailing_pe, forward_pe, price_to_book, and beta.
- Always round dollar values to two decimal places and percentages to two decimal places.
- When a question references a time period, filter results to that specific period. If no time period is specified, default to the most recent fiscal year or most recent trading data available.
- For sector-level analysis, use the sector column from vw_company_fundamentals or vw_stock_performance.

CLARIFICATION RULES:
- When users ask about "performance" without specifying stock price or financials, ask for clarification.
- When users ask about "top companies" without specifying the metric, ask which metric to rank by.
- When users ask about a specific company without specifying a fiscal year, ask which year.
- When users ask about economic indicators without specifying a country, ask which countries.

DOMAIN TERMINOLOGY:
- "Large cap" means market_cap > 100B USD; "Mid cap" = 10B–100B; "Small cap" < 10B
- "High margin" means net_margin_pct > 20%; "Profitable" means net_income > 0
- "Revenue growth" = revenue_growth_pct column
- "Top line" / "sales" = revenue; "Bottom line" / "earnings" = net_income
- "PE ratio" / "P/E" = trailing_pe or forward_pe columns; "PB ratio" / "P/B" = price_to_book
- "ROE" = return_on_equity_pct; "ROA" = return_on_assets_pct; "Leverage" = debt_to_asset_ratio
- "Dividend yield" = dividend_yield column (percentage); "Payout ratio" = payout_ratio column
- "Alpha" / "excess return" = excess_return_pct from vw_stock_vs_benchmark
- "Beta" = beta column in dim_companies; measures stock volatility relative to the market

SUMMARY FORMATTING:
- Always include the date range or fiscal year covered.
- Use bullet points for multi-part summaries.
- Round currency to two decimal places in readable format (e.g. "$1.23B").
- Mention trend direction when results span multiple years."""

# COMMAND ----------

FQN = f"{CATALOG}.{SCHEMA}"

EXAMPLE_QUERIES = [
    {
        "question": ["What are the top companies by revenue?"],
        "sql": [
            f"SELECT ticker, company_name, sector, fiscal_year, revenue, net_income, net_margin_pct\n",
            f"FROM {FQN}.vw_company_fundamentals\n",
            f"WHERE fiscal_year = (SELECT MAX(fiscal_year) FROM {FQN}.vw_company_fundamentals)\n",
            f"ORDER BY revenue DESC\n",
            f"LIMIT 10;\n",
        ],
    },
    {
        "question": ["Show me Apple's revenue over time"],
        "sql": [
            f"SELECT ticker, company_name, fiscal_year, revenue, net_income, revenue_growth_pct, net_margin_pct\n",
            f"FROM {FQN}.vw_company_fundamentals\n",
            f"WHERE ticker = 'AAPL'\n",
            f"ORDER BY fiscal_year;\n",
        ],
    },
    {
        "question": ["Which sector has the highest profit margin?"],
        "sql": [
            f"SELECT sector,\n",
            f"  ROUND(AVG(net_margin_pct), 2) AS avg_net_margin_pct,\n",
            f"  ROUND(AVG(return_on_equity_pct), 2) AS avg_roe_pct,\n",
            f"  COUNT(DISTINCT ticker) AS num_companies\n",
            f"FROM {FQN}.vw_company_fundamentals\n",
            f"WHERE fiscal_year = (SELECT MAX(fiscal_year) FROM {FQN}.vw_company_fundamentals)\n",
            f"GROUP BY sector\n",
            f"ORDER BY avg_net_margin_pct DESC;\n",
        ],
    },
    {
        "question": ["What are the best performing stocks this year?"],
        "sql": [
            f"WITH ytd AS (\n",
            f"  SELECT ticker, company_name, sector,\n",
            f"    FIRST_VALUE(close_price) OVER (PARTITION BY ticker ORDER BY trade_date) AS first_price,\n",
            f"    LAST_VALUE(close_price) OVER (PARTITION BY ticker ORDER BY trade_date\n",
            f"      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_price\n",
            f"  FROM {FQN}.vw_stock_performance\n",
            f"  WHERE trade_year = YEAR(CURRENT_DATE())\n",
            f")\n",
            f"SELECT DISTINCT ticker, company_name, sector,\n",
            f"  ROUND(((last_price - first_price) / first_price) * 100, 2) AS ytd_return_pct\n",
            f"FROM ytd WHERE first_price > 0\n",
            f"ORDER BY ytd_return_pct DESC LIMIT 10;\n",
        ],
    },
    {
        "question": ["Which stocks are the most volatile?"],
        "sql": [
            f"SELECT ticker, company_name, sector,\n",
            f"  ROUND(STDDEV(daily_return_pct), 4) AS daily_return_stddev,\n",
            f"  ROUND(MIN(daily_return_pct), 2) AS worst_day,\n",
            f"  ROUND(MAX(daily_return_pct), 2) AS best_day\n",
            f"FROM {FQN}.vw_stock_performance\n",
            f"WHERE trade_year = YEAR(CURRENT_DATE()) - 1\n",
            f"GROUP BY ticker, company_name, sector\n",
            f"ORDER BY daily_return_stddev DESC LIMIT 10;\n",
        ],
    },
    {
        "question": ["What is the GDP growth trend for the United States?"],
        "sql": [
            f"SELECT country, year, indicator_name, value\n",
            f"FROM {FQN}.vw_economic_overview\n",
            f"WHERE country = 'United States'\n",
            f"  AND indicator_name = 'GDP Growth Rate (%)'\n",
            f"ORDER BY year;\n",
        ],
    },
    {
        "question": ["Compare inflation rates across countries"],
        "sql": [
            f"SELECT country, year, value AS inflation_rate_pct\n",
            f"FROM {FQN}.vw_economic_overview\n",
            f"WHERE indicator_name = 'Inflation Rate (%)'\n",
            f"  AND year = (SELECT MAX(year) FROM {FQN}.vw_economic_overview\n",
            f"              WHERE indicator_name = 'Inflation Rate (%)')\n",
            f"ORDER BY value DESC;\n",
        ],
    },
    {
        "question": ["Which companies are highly leveraged?"],
        "sql": [
            f"SELECT ticker, company_name, sector, fiscal_year,\n",
            f"  debt_to_asset_ratio, total_liabilities, total_assets\n",
            f"FROM {FQN}.vw_company_fundamentals\n",
            f"WHERE fiscal_year = (SELECT MAX(fiscal_year) FROM {FQN}.vw_company_fundamentals)\n",
            f"  AND debt_to_asset_ratio > 0.7\n",
            f"ORDER BY debt_to_asset_ratio DESC;\n",
        ],
    },
    {
        "question": ["How did Apple perform relative to the S&P 500?"],
        "sql": [
            f"SELECT ticker, company_name,\n",
            f"  ROUND(AVG(stock_daily_return_pct), 4) AS avg_daily_return,\n",
            f"  ROUND(AVG(sp500_daily_return_pct), 4) AS avg_sp500_return,\n",
            f"  ROUND(AVG(excess_return_pct), 4) AS avg_excess_return,\n",
            f"  COUNT(*) AS trading_days\n",
            f"FROM {FQN}.vw_stock_vs_benchmark\n",
            f"WHERE ticker = 'AAPL'\n",
            f"  AND trade_year = YEAR(CURRENT_DATE()) - 1\n",
            f"GROUP BY ticker, company_name;\n",
        ],
    },
    {
        "question": ["Which companies have the highest dividend yield?"],
        "sql": [
            f"SELECT ticker, company_name, sector,\n",
            f"  ROUND(dividend_yield, 2) AS dividend_yield_pct,\n",
            f"  ROUND(dividend_rate, 2) AS annual_dividend_usd,\n",
            f"  ROUND(payout_ratio, 1) AS payout_ratio_pct\n",
            f"FROM {FQN}.vw_company_fundamentals\n",
            f"WHERE dividend_yield > 0\n",
            f"  AND fiscal_year = (SELECT MAX(fiscal_year) FROM {FQN}.vw_company_fundamentals)\n",
            f"ORDER BY dividend_yield DESC\n",
            f"LIMIT 10;\n",
        ],
    },
]

SAMPLE_QUESTIONS = [
    "What are the top 10 companies by revenue?",
    "Show me Apple's revenue trend over the last 5 years",
    "Which sector has the highest average profit margin?",
    "How did Tesla stock perform relative to the S&P 500 last year?",
    "What is the GDP growth trend for the United States?",
    "Which companies have the highest dividend yield?",
    "Which companies have the highest return on equity?",
    "Compare inflation rates across major economies",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Snippets
# MAGIC
# MAGIC These teach Genie reusable business terms so it can translate
# MAGIC natural language like "high margin" or "PE ratio" into correct SQL.
# MAGIC The API organises them into three categories:
# MAGIC - **expressions** — computed columns (alias + SQL)
# MAGIC - **measures** — aggregate metrics (alias + SQL)
# MAGIC - **filters** — boolean conditions (display_name + SQL)

# COMMAND ----------

SQL_SNIPPET_EXPRESSIONS = [
    {"alias": "revenue_growth_pct", "sql": ["ROUND(((revenue - prev_year_revenue) / NULLIF(prev_year_revenue, 0)) * 100, 2)"]},
    {"alias": "profit_margin_pct", "sql": ["ROUND((net_income / NULLIF(revenue, 0)) * 100, 2)"]},
    {"alias": "return_on_equity_pct", "sql": ["ROUND((net_income / NULLIF(stockholders_equity, 0)) * 100, 2)"]},
    {"alias": "return_on_assets_pct", "sql": ["ROUND((net_income / NULLIF(total_assets, 0)) * 100, 2)"]},
    {"alias": "earnings_per_share", "sql": ["eps"]},
    {"alias": "pe_ratio", "sql": ["CASE WHEN net_income > 0 THEN ROUND(market_cap / net_income, 2) ELSE NULL END"]},
    {"alias": "price_to_revenue", "sql": ["CASE WHEN revenue > 0 THEN ROUND(market_cap / revenue, 2) ELSE NULL END"]},
    {"alias": "price_to_book", "sql": ["CASE WHEN stockholders_equity > 0 THEN ROUND(market_cap / stockholders_equity, 2) ELSE NULL END"]},
    {"alias": "debt_to_asset_ratio", "sql": ["ROUND(total_liabilities / NULLIF(total_assets, 0), 2)"]},
    {"alias": "debt_to_equity_ratio", "sql": ["CASE WHEN stockholders_equity > 0 THEN ROUND(total_liabilities / stockholders_equity, 2) ELSE NULL END"]},
    {"alias": "equity_ratio", "sql": ["ROUND(stockholders_equity / NULLIF(total_assets, 0), 2)"]},
    {"alias": "daily_return", "sql": ["daily_return_pct"]},
    {"alias": "price_range", "sql": ["ROUND(high_price - low_price, 2)"]},
    {"alias": "price_range_pct", "sql": ["ROUND(((high_price - low_price) / NULLIF(open_price, 0)) * 100, 2)"]},
]

SQL_SNIPPET_MEASURES = [
    {"alias": "avg_daily_volume", "sql": ["AVG(volume)"]},
    {"alias": "avg_excess_return", "sql": ["ROUND(AVG(excess_return_pct), 4)"]},
]

SQL_SNIPPET_FILTERS = [
    {"display_name": "large cap", "sql": ["market_cap > 100000000000"]},
    {"display_name": "mid cap", "sql": ["market_cap BETWEEN 10000000000 AND 100000000000"]},
    {"display_name": "small cap", "sql": ["market_cap < 10000000000"]},
    {"display_name": "profitable", "sql": ["net_income > 0"]},
    {"display_name": "high growth", "sql": ["revenue_growth_pct > 15"]},
    {"display_name": "high ROE", "sql": ["return_on_equity_pct > 20"]},
    {"display_name": "highly leveraged", "sql": ["debt_to_asset_ratio > 0.7"]},
    {"display_name": "dividend payer", "sql": ["dividend_yield > 0"]},
    {"display_name": "high yield", "sql": ["dividend_yield > 3"]},
    {"display_name": "low beta", "sql": ["beta < 0.8"]},
    {"display_name": "high beta", "sql": ["beta > 1.2"]},
    {"display_name": "undervalued", "sql": ["trailing_pe < 15"]},
]

# COMMAND ----------

serialized_space = {
    "version": 1,
    "data_sources": {
        "tables": [
            {"identifier": f"{FQN}.vw_company_fundamentals"},
            {"identifier": f"{FQN}.vw_stock_performance"},
            {"identifier": f"{FQN}.vw_stock_vs_benchmark"},
            {"identifier": f"{FQN}.vw_economic_overview"},
        ]
    },
    "instructions": {
        "text_instructions": [
            {"id": hex_id(), "content": [INSTRUCTIONS]}
        ],
        "example_question_sqls": [
            {"id": hex_id(), "question": eq["question"], "sql": eq["sql"]}
            for eq in EXAMPLE_QUERIES
        ],
        "sql_snippets": {
            "expressions": [
                {"id": hex_id(), "alias": e["alias"], "sql": e["sql"]}
                for e in SQL_SNIPPET_EXPRESSIONS
            ],
            "measures": [
                {"id": hex_id(), "alias": m["alias"], "sql": m["sql"]}
                for m in SQL_SNIPPET_MEASURES
            ],
            "filters": [
                {"id": hex_id(), "display_name": f["display_name"], "sql": f["sql"]}
                for f in SQL_SNIPPET_FILTERS
            ],
        },
    },
    "config": {
        "sample_questions": [
            {"id": hex_id(), "question": [q]}
            for q in SAMPLE_QUESTIONS
        ]
    },
}

payload = {
    "title": SPACE_TITLE,
    "description": "Ask questions about S&P 500 company financials, stock price performance, and macroeconomic indicators. Designed for investment research and portfolio analysis.",
    "warehouse_id": WAREHOUSE_ID,
    "serialized_space": json.dumps(serialized_space),
}

print("Payload built. Tables included:")
for t in serialized_space["data_sources"]["tables"]:
    print(f"  - {t['identifier']}")
print(f"Example queries:  {len(EXAMPLE_QUERIES)}")
print(f"SQL snippets:     {len(SQL_SNIPPET_EXPRESSIONS)} expressions, {len(SQL_SNIPPET_MEASURES)} measures, {len(SQL_SNIPPET_FILTERS)} filters")
print(f"Sample questions: {len(SAMPLE_QUESTIONS)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the Genie space using the Databricks SDK
# MAGIC
# MAGIC `WorkspaceClient` handles authentication automatically inside a notebook.
# MAGIC The `api_client.do()` method provides full REST access with built-in
# MAGIC auth, retries, and error handling — no raw tokens or `requests` needed.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Check for existing space with the same title

# COMMAND ----------

existing_space_id = None

spaces = w.api_client.do("GET", "/api/2.0/genie/spaces").get("spaces", [])
for space in spaces:
    if space.get("title") == SPACE_TITLE:
        existing_space_id = space["space_id"]
        print(f"Found existing space '{SPACE_TITLE}' (ID: {existing_space_id}). Will update in place.")
        break

if not existing_space_id:
    print(f"No existing space named '{SPACE_TITLE}'. Will create a new one.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Create or update the Genie space

# COMMAND ----------

if existing_space_id:
    result = w.api_client.do("PATCH", f"/api/2.0/genie/spaces/{existing_space_id}", body=payload)
    action = "updated"
else:
    result = w.api_client.do("POST", "/api/2.0/genie/spaces/", body=payload)
    action = "created"

space_id = result.get("space_id", "unknown")
space_url = f"{w.config.host}/genie/rooms/{space_id}"

print(f"Genie space {action} successfully!")
print(f"  Space ID : {space_id}")
print(f"  Title    : {result.get('title', '')}")
print(f"  URL      : {space_url}")

dbutils.notebook.exit(json.dumps({"space_id": space_id, "url": space_url}))
