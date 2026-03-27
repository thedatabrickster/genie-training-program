# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # Genie Training Program — Prerequisites Setup
# MAGIC
# MAGIC This notebook prepares the workspace for the Genie training pipeline.
# MAGIC It is **idempotent** — safe to run multiple times.
# MAGIC
# MAGIC **What it does:**
# MAGIC 1. Creates the Unity Catalog catalog (if it doesn't exist)
# MAGIC 2. Creates the schema
# MAGIC 3. Creates a managed Volume for the IMF data file
# MAGIC 4. Uploads the WEO TSV file from the bundled workspace files into the Volume
# MAGIC
# MAGIC **Run this once** before running the data setup job.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "genie")

CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()
VOLUME_NAME = "imf_data"
TSV_FILE = "WEOOct2024all_utf8.tsv"

print(f"Catalog : {CATALOG}")
print(f"Schema  : {CATALOG}.{SCHEMA}")
print(f"Volume  : {CATALOG}.{SCHEMA}.{VOLUME_NAME}")
print(f"File    : {TSV_FILE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Catalog

# COMMAND ----------

try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    print(f"Catalog '{CATALOG}' — ready")
except Exception as e:
    if "already exists" in str(e).lower() or "CATALOG_ALREADY_EXISTS" in str(e):
        print(f"Catalog '{CATALOG}' already exists — skipping")
    elif "PERMISSION_DENIED" in str(e) or "does not exist" in str(e).lower():
        print(f"Cannot create catalog (likely permission denied) — assuming it exists")
        print(f"  If this fails later, ask your workspace admin to create catalog '{CATALOG}'")
    else:
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Schema

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
print(f"Schema '{CATALOG}.{SCHEMA}' — ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Volume

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME_NAME}")
print(f"Volume '{CATALOG}.{SCHEMA}.{VOLUME_NAME}' — ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Upload IMF WEO Data File
# MAGIC
# MAGIC Downloads the TSV file from the Git repository and uploads it to the Volume.

# COMMAND ----------

import requests

volume_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}/{TSV_FILE}"

# Check if file already exists in Volume
try:
    files = dbutils.fs.ls(f"dbfs:/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}/")
    existing = [f.name for f in files if TSV_FILE in f.name]
    if existing:
        print(f"File already exists in Volume at: {volume_path}")
        print("Skipping download and upload.")
    else:
        # Download from Git repo
        git_url = f"https://raw.githubusercontent.com/thedatabrickster/genie-training-program/main/01_data_setup/{TSV_FILE}"
        response = requests.get(git_url)
        if response.status_code == 200:
            # Write to volume
            with open(f"/dbfs{volume_path}", "wb") as f:
                f.write(response.content)
            print(f"Downloaded and uploaded: {git_url}")
            print(f"                  →  {volume_path}")
        else:
            print(f"Failed to download from {git_url} (status: {response.status_code})")
except Exception as e:
    print(f"Error handling file: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

print("=== Prerequisites Validation ===\n")

catalog_ok = False
try:
    spark.sql(f"USE CATALOG {CATALOG}")
    catalog_ok = True
    print(f"  ✓ Catalog '{CATALOG}' accessible")
except Exception:
    print(f"  ✗ Catalog '{CATALOG}' NOT accessible")

schema_ok = False
try:
    spark.sql(f"USE SCHEMA {SCHEMA}")
    schema_ok = True
    print(f"  ✓ Schema '{CATALOG}.{SCHEMA}' accessible")
except Exception:
    print(f"  ✗ Schema '{CATALOG}.{SCHEMA}' NOT accessible")

volume_ok = False
try:
    files = dbutils.fs.ls(f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME_NAME}/")
    tsv_found = any(TSV_FILE in f.name for f in files)
    if tsv_found:
        volume_ok = True
        print(f"  ✓ Volume '{VOLUME_NAME}' exists and contains {TSV_FILE}")
    else:
        print(f"  ✗ Volume '{VOLUME_NAME}' exists but {TSV_FILE} not found")
except Exception:
    print(f"  ✗ Volume '{VOLUME_NAME}' NOT accessible")

print()
if catalog_ok and schema_ok and volume_ok:
    print("All prerequisites are met. Ready to run the data setup job.")
else:
    print("Some prerequisites are missing — check the errors above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Lakeflow Job
# MAGIC
# MAGIC Creates the "Genie Training - Data Setup" job if it doesn't exist.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

job_name = "Genie Training - Data Setup"

# Check if job already exists
existing_jobs = list(w.jobs.list(name=job_name))
if existing_jobs:
    job_id = existing_jobs[0].job_id
    print(f"Job '{job_name}' already exists (ID: {job_id}). Deleting to recreate...")
    w.jobs.delete(job_id)
    print(f"Deleted job {job_id}.")

# Find the best SQL warehouse — prefer serverless over pro/classic
warehouses = list(w.warehouses.list())
if not warehouses:
    raise Exception("No SQL warehouse found. Create a serverless SQL warehouse first.")

def _serverless_priority(wh):
    """Rank: serverless (0) > pro-with-serverless (1) > pro (2) > classic (3)."""
    wh_type = str(getattr(wh, "warehouse_type", "")).upper()
    if "SERVERLESS" in wh_type:
        return 0
    if getattr(wh, "enable_serverless_compute", False):
        return 1
    if "PRO" in wh_type:
        return 2
    return 3

warehouses.sort(key=_serverless_priority)
selected_wh = warehouses[0]
print(f"Using SQL warehouse: {selected_wh.name} (ID: {selected_wh.id})")

# Define the job — fully serverless
job_spec = {
    "name": job_name,
    "tags": {
        "project": "genie_training",
        "team": "analytics"
    },
    "git_source": {
        "git_url": "https://github.com/thedatabrickster/genie-training-program",
        "git_provider": "gitHub",
        "git_branch": "main"
    },
    "tasks": [
        {
            "task_key": "load_stock_data",
            "description": "Pull S&P 500 stock data, financials, dividends, valuations, and benchmark via yfinance",
            "notebook_task": {
                "notebook_path": "01_data_setup/02_load_stock_data",
                "base_parameters": {
                    "catalog": CATALOG,
                    "schema": SCHEMA
                }
            },
            "timeout_seconds": 3600
        },
        {
            "task_key": "load_economic_data",
            "description": "Load IMF World Economic Outlook data for 20 major economies from Volume",
            "notebook_task": {
                "notebook_path": "01_data_setup/03_load_economic_data",
                "base_parameters": {
                    "catalog": CATALOG,
                    "schema": SCHEMA
                }
            },
            "timeout_seconds": 1800
        },
        {
            "task_key": "create_data_model",
            "description": "Build pre-joined Genie views from upstream tables",
            "depends_on": [
                {"task_key": "load_stock_data"},
                {"task_key": "load_economic_data"}
            ],
            "sql_task": {
                "warehouse_id": selected_wh.id,
                "file": {
                    "path": "01_data_setup/04_create_data_model.sql"
                },
                "parameters": {
                    "catalog": CATALOG,
                    "schema": SCHEMA
                }
            },
            "timeout_seconds": 1800
        },
        {
            "task_key": "grant_genie_access",
            "description": "Grant Unity Catalog access needed to open the Genie space",
            "depends_on": [
                {"task_key": "create_data_model"}
            ],
            "notebook_task": {
                "notebook_path": "01_data_setup/05_grant_genie_access",
                "base_parameters": {
                    "catalog": CATALOG,
                    "schema": SCHEMA,
                    "consumer_principal": ""
                }
            },
            "timeout_seconds": 600
        }
    ]
}

result = w.api_client.do("POST", "/api/2.1/jobs/create", body=job_spec)
print(f"\nJob '{job_name}' created (ID: {result.get('job_id', 'unknown')}).")
print(f"All notebook tasks run on serverless compute.")
print(f"SQL task uses warehouse: {selected_wh.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Done!
# MAGIC
# MAGIC **Next steps:**
# MAGIC 1. The **Genie Training - Data Setup** Lakeflow job has been created and is ready to run.
# MAGIC    - It includes tasks: load stock data, load economic data, create data model, grant Genie access.
# MAGIC 2. Run the job from the Databricks Jobs UI or via API.