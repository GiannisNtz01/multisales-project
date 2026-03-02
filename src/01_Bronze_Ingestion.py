# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze Ingestion Layer
# MAGIC
# MAGIC ## Objective
# MAGIC Ingest raw CSV exports from the Unity Catalog Volume into Delta tables.
# MAGIC
# MAGIC ### Bronze Principles:
# MAGIC - Preserve raw data (auditability)
# MAGIC - Apply minimal column name sanitization only
# MAGIC - No business logic or cleansing (handled in Silver)
# MAGIC - Store as Delta tables in Unity Catalog
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
TARGET_CATALOG = "multisales_catalog"
TARGET_SCHEMA = "bronze"
base = "/Volumes/multisales_catalog/bronze/raw_files"
DELIM = "\t"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Helper Functions
# MAGIC
# MAGIC We define:
# MAGIC - read_csv(): reads tab-delimited raw CSV files from Volume
# MAGIC - sanitize_minimal(): cleans column names only (not values)

# COMMAND ----------

def read_csv(name: str):
    return (
        spark.read
            .option("header", True)
            .option("delimiter", DELIM)
            .csv(f"{base}/{name}")
    )

invalid_chars = " ,;{}()\n\t="

def sanitize_minimal(df):
    for c in df.columns:
        new_c = c.strip()
        for ch in invalid_chars:
            new_c = new_c.replace(ch, "_")
        while "__" in new_c:
            new_c = new_c.replace("__", "_")
        new_c = new_c.strip("_")

        if new_c != c:
            df = df.withColumnRenamed(c, new_c)
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Sanity Check
# MAGIC
# MAGIC Verify:
# MAGIC - Files exist in Volume
# MAGIC - Delimiter produces correct column split

# COMMAND ----------

display(dbutils.fs.ls(base))

test = read_csv("Sales_2017.csv")
display(test.limit(5))

print("Column count:", len(test.columns))
print(test.columns)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Read & Union Yearly Sales Files
# MAGIC
# MAGIC We:
# MAGIC - Load Sales_2017 to Sales_2020
# MAGIC - Add source_year for traceability
# MAGIC - Union datasets using unionByName
# MAGIC - Apply minimal column sanitization

# COMMAND ----------

sales_2017 = read_csv("Sales_2017.csv").withColumn("source_year", F.lit(2017))
sales_2018 = read_csv("Sales_2018.csv").withColumn("source_year", F.lit(2018))
sales_2019 = read_csv("Sales_2019.csv").withColumn("source_year", F.lit(2019))
sales_2020 = read_csv("Sales_2020.csv").withColumn("source_year", F.lit(2020))

bronze_sales = (
    sales_2017
        .unionByName(sales_2018, allowMissingColumns=True)
        .unionByName(sales_2019, allowMissingColumns=True)
        .unionByName(sales_2020, allowMissingColumns=True)
)

bronze_sales = sanitize_minimal(bronze_sales)

display(bronze_sales.limit(5))
print("Columns:", len(bronze_sales.columns))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — Persist Bronze Sales Table
# MAGIC
# MAGIC Write the unified dataset as Delta table in Unity Catalog.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {TARGET_CATALOG}.{TARGET_SCHEMA}")

(
    bronze_sales.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.bronze_sales")
)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Ingest Reference / Master Data
# MAGIC
# MAGIC Load supporting datasets

# COMMAND ----------

bronze_product = sanitize_minimal(read_csv("Product.csv"))
bronze_reseller = sanitize_minimal(read_csv("Reseller.csv"))
bronze_region = sanitize_minimal(read_csv("Region.csv"))
bronze_salesperson = sanitize_minimal(read_csv("Salesperson.csv"))
bronze_salespersonregion = sanitize_minimal(read_csv("SalespersonRegion.csv"))
bronze_targets = sanitize_minimal(read_csv("Targets.csv"))

display(bronze_product.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — Persist Bronze Reference Tables

# COMMAND ----------

tables = {
    "bronze_product": bronze_product,
    "bronze_reseller": bronze_reseller,
    "bronze_region": bronze_region,
    "bronze_salesperson": bronze_salesperson,
    "bronze_salespersonregion": bronze_salespersonregion,
    "bronze_targets": bronze_targets,
}

for tname, df in tables.items():
    (
        df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{tname}")
    )
    print("Created table:", f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{tname}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Verify Bronze Layer

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {TARGET_CATALOG}.{TARGET_SCHEMA}"))

display(spark.sql(f"""
SELECT *
FROM {TARGET_CATALOG}.{TARGET_SCHEMA}.bronze_sales
LIMIT 5
"""))
