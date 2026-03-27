# Databricks notebook source

# MAGIC %md
# MAGIC # Load IMF World Economic Outlook Data
# MAGIC
# MAGIC This notebook reads the IMF World Economic Outlook (WEO) dataset from a
# MAGIC Unity Catalog Volume and loads key macroeconomic indicators into a fact table.
# MAGIC
# MAGIC **Source:** [IMF WEO Database](https://data.imf.org/en/Datasets/WEO) — October 2024 release
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - WEO file uploaded to `/Volumes/{catalog}/{schema}/imf_data/WEOOct2024all_utf8.tsv`
# MAGIC   (the Volume is created automatically; you only need to upload the file)
# MAGIC - Unity Catalog enabled on the workspace

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "training")
dbutils.widgets.text("schema", "genie")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")

VOLUME_NAME = "imf_data"
FILE_NAME = "WEOOct2024all_utf8.tsv"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}/{FILE_NAME}"

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME_NAME}")

print(f"Target: {CATALOG}.{SCHEMA}")
print(f"Volume: {CATALOG}.{SCHEMA}.{VOLUME_NAME}")
print(f"Source: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Read the WEO File from Volume
# MAGIC
# MAGIC The WEO file is a UTF-8 tab-delimited TSV (converted from the original
# MAGIC IMF UTF-16-LE release). If the file is not found in the Volume, upload it
# MAGIC from `01_data_setup/WEOOct2024all_utf8.tsv` using:
# MAGIC ```
# MAGIC databricks fs cp 01_data_setup/WEOOct2024all_utf8.tsv dbfs:/Volumes/<catalog>/<schema>/imf_data/WEOOct2024all_utf8.tsv
# MAGIC ```

# COMMAND ----------

raw_df = (
    spark.read
    .option("header", "true")
    .option("sep", "\t")
    .option("inferSchema", "false")
    .csv(VOLUME_PATH)
)

total_rows = raw_df.count()
total_countries = raw_df.select("Country").distinct().count()
print(f"Raw WEO dataset: {total_rows} rows, {total_countries} countries")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Filter and Reshape

# COMMAND ----------

INDICATORS = {
    "NGDP_RPCH":   "GDP Growth Rate (%)",
    "PCPIPCH":     "Inflation Rate (%)",
    "LUR":         "Unemployment Rate (%)",
    "GGXWDG_NGDP": "Government Debt (% of GDP)",
    "BCA_NGDPD":   "Current Account Balance (% of GDP)",
}

COUNTRY_RENAME = {
    "Korea": "South Korea",
}

TARGET_COUNTRIES = [
    "United States", "United Kingdom", "China", "Japan", "Germany",
    "France", "India", "Canada", "Australia", "Brazil", "Mexico",
    "Italy", "Spain", "Korea", "Switzerland", "Netherlands",
    "Sweden", "Singapore", "Hong Kong SAR", "Indonesia",
]

YEAR_COLS = [str(y) for y in range(2015, 2031)]

filtered = raw_df.filter(
    (F.col("WEO Subject Code").isin(list(INDICATORS.keys()))) &
    (F.col("Country").isin(TARGET_COUNTRIES))
)

print(f"Filtered to {filtered.count()} rows")

# COMMAND ----------

year_exprs = []
for y in YEAR_COLS:
    if y in raw_df.columns:
        year_exprs.append(y)

stack_expr = ", ".join([f"'{y}', `{y}`" for y in year_exprs])
n = len(year_exprs)

unpivoted = filtered.select(
    F.col("ISO").alias("country_code"),
    F.col("Country").alias("country_raw"),
    F.col("WEO Subject Code").alias("indicator_code"),
    F.col("Estimates Start After").alias("estimates_start"),
    F.expr(f"stack({n}, {stack_expr}) as (year_str, value_str)")
)

indicator_map_expr = F.create_map([F.lit(x) for pair in INDICATORS.items() for x in pair])
country_rename_expr = F.create_map([F.lit(x) for pair in COUNTRY_RENAME.items() for x in pair])

result = (
    unpivoted
    .filter(
        F.col("value_str").isNotNull() &
        (F.col("value_str") != "n/a") &
        (F.col("value_str") != "--")
    )
    .withColumn("value_clean", F.regexp_replace(F.col("value_str"), ",", ""))
    .withColumn("value", F.col("value_clean").cast(DoubleType()))
    .filter(F.col("value").isNotNull())
    .withColumn("year", F.col("year_str").cast("int"))
    .withColumn("indicator_name", indicator_map_expr[F.col("indicator_code")])
    .withColumn("country", F.coalesce(
        country_rename_expr[F.col("country_raw")],
        F.col("country_raw")
    ))
    .withColumn("value", F.round(F.col("value"), 3))
    .withColumn("is_projection",
        F.when(
            F.col("estimates_start").isNotNull() &
            (F.col("year") > F.col("estimates_start").cast("int")),
            F.lit("Y")
        ).otherwise(F.lit("N"))
    )
    .select(
        "country_code", "country", "indicator_code", "indicator_name",
        "year", "value", "is_projection"
    )
)

print(f"Total records after unpivot: {result.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Write to Unity Catalog

# COMMAND ----------

result.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.{SCHEMA}.fact_economic_indicators"
)

row_count = spark.table(f"{CATALOG}.{SCHEMA}.fact_economic_indicators").count()
print(f"Created {CATALOG}.{SCHEMA}.fact_economic_indicators with {row_count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Apply Unity Catalog Metadata

# COMMAND ----------

spark.sql(f"""
    ALTER TABLE {CATALOG}.{SCHEMA}.fact_economic_indicators
    SET TBLPROPERTIES (
        'comment' = 'Key macroeconomic indicators for 20 major world economies sourced from the IMF World Economic Outlook (October 2024). Covers GDP growth, inflation, unemployment, government debt, and current account balance from 2015 to 2029. One row per country per indicator per year.'
    )
""")

column_comments = {
    "country_code": "ISO 3-letter country code (e.g., USA, GBR, JPN, CHN)",
    "country": "Full country name",
    "indicator_code": "IMF WEO subject code (e.g., NGDP_RPCH for GDP growth, PCPIPCH for inflation, LUR for unemployment)",
    "indicator_name": "Human-readable indicator name (e.g., GDP Growth Rate (%), Inflation Rate (%))",
    "year": "Calendar year of the observation (2015-2029)",
    "value": "Numeric value of the indicator. Units depend on indicator_name — most are percentages.",
    "is_projection": "Y if the value is an IMF projection/estimate, N if it is an actual/reported value",
}

for col, comment in column_comments.items():
    spark.sql(f"ALTER TABLE {CATALOG}.{SCHEMA}.fact_economic_indicators ALTER COLUMN {col} COMMENT '{comment}'")

print("Table and column comments applied.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Validate

# COMMAND ----------

fqn = f"{CATALOG}.{SCHEMA}.fact_economic_indicators"

print("=== Data Validation ===\n")
total = spark.table(fqn).count()
countries = spark.table(fqn).select("country").distinct().count()
indicators = spark.table(fqn).select("indicator_name").distinct().count()
year_range = spark.table(fqn).agg(
    F.min("year").alias("min_year"),
    F.max("year").alias("max_year")
).collect()[0]

print(f"  Total rows:    {total:,}")
print(f"  Countries:     {countries}")
print(f"  Indicators:    {indicators}")
print(f"  Year range:    {year_range['min_year']} - {year_range['max_year']}")

print("\n=== Records by Country ===")
display(
    spark.table(fqn)
    .groupBy("country")
    .agg(F.count("*").alias("records"), F.countDistinct("indicator_name").alias("indicators"))
    .orderBy(F.desc("records"))
)

print("\n=== Sample: US GDP Growth ===")
display(
    spark.table(fqn)
    .filter((F.col("country") == "United States") & (F.col("indicator_name") == "GDP Growth Rate (%)"))
    .orderBy("year")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done!
# MAGIC
# MAGIC The `fact_economic_indicators` table now contains real IMF data covering:
# MAGIC
# MAGIC | Indicator | Description |
# MAGIC |-----------|-------------|
# MAGIC | GDP Growth Rate (%) | Year-over-year real GDP growth |
# MAGIC | Inflation Rate (%) | Average consumer price inflation |
# MAGIC | Unemployment Rate (%) | Percent of total labor force |
# MAGIC | Government Debt (% of GDP) | General government gross debt |
# MAGIC | Current Account Balance (% of GDP) | Trade and income balance |
# MAGIC
# MAGIC **Countries:** United States, United Kingdom, China, Japan, Germany, France,
# MAGIC India, Canada, Australia, Brazil, Mexico, Italy, Spain, South Korea,
# MAGIC Switzerland, Netherlands, Sweden, Singapore, Hong Kong SAR, Indonesia
# MAGIC
# MAGIC **Next step:** Run `04_create_data_model.sql` to build the views for Genie.
