# Databricks notebook source
# MAGIC %md
# MAGIC # Step 5: Grant Genie Consumer Access
# MAGIC
# MAGIC This notebook grants the Unity Catalog privileges required for a Genie
# MAGIC consumer to open the space and read the curated views.
# MAGIC
# MAGIC If no `consumer_principal` is provided, it auto-detects the current user.

# COMMAND ----------

dbutils.widgets.text("catalog", "training")
dbutils.widgets.text("schema", "genie")
dbutils.widgets.text("consumer_principal", "")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
CONSUMER_PRINCIPAL = dbutils.widgets.get("consumer_principal").strip()

if not CONSUMER_PRINCIPAL:
    CONSUMER_PRINCIPAL = spark.sql("SELECT current_user()").collect()[0][0]
    print(f"No principal specified — auto-detected: {CONSUMER_PRINCIPAL}")

print(f"Catalog:             {CATALOG}")
print(f"Schema:              {SCHEMA}")
print(f"Consumer principal:  {CONSUMER_PRINCIPAL}")

# COMMAND ----------

import json


def quote_identifier(value: str) -> str:
    return f"`{value.replace('`', '``')}`"


quoted_catalog = quote_identifier(CATALOG)
quoted_schema = quote_identifier(SCHEMA)
quoted_principal = quote_identifier(CONSUMER_PRINCIPAL)

object_grants = [
    "dim_companies",
    "dim_sectors",
    "fact_economic_indicators",
    "fact_financials",
    "fact_stock_prices",
    "fact_sp500_benchmark",
    "vw_company_fundamentals",
    "vw_stock_performance",
    "vw_stock_vs_benchmark",
    "vw_economic_overview",
]

statements = [
    ("USE CATALOG", f"GRANT USE CATALOG ON CATALOG {quoted_catalog} TO {quoted_principal}"),
    ("USE SCHEMA", f"GRANT USE SCHEMA ON SCHEMA {quoted_catalog}.{quoted_schema} TO {quoted_principal}"),
    ("SELECT ON SCHEMA", f"GRANT SELECT ON SCHEMA {quoted_catalog}.{quoted_schema} TO {quoted_principal}"),
]

statements.extend(
    [
        (
            f"SELECT ON TABLE {object_name}",
            f"GRANT SELECT ON TABLE {quoted_catalog}.{quoted_schema}.{object_name} TO {quoted_principal}",
        )
        for object_name in object_grants
    ]
)

granted = []
warnings = []

for label, statement in statements:
    print(statement)
    try:
        spark.sql(statement)
        granted.append(label)
    except Exception as exc:
        message = str(exc)[:300]
        warnings.append({"privilege": label, "error": message})
        print(f"WARNING: could not grant {label}: {message}")

print(
    json.dumps(
        {
            "granted_to": CONSUMER_PRINCIPAL,
            "catalog": CATALOG,
            "schema": SCHEMA,
            "granted": granted,
            "warnings": warnings,
        }
    )
)
