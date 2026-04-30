# Databricks notebook source
# MAGIC %md
# MAGIC # Load Vendor Lookup Table
# MAGIC
# MAGIC One-time setup: upload the vendor CSV to a UC Volume, then run this notebook
# MAGIC to load it as a Delta table. The vendor matching pipeline reads from this table.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Path to the vendor CSV file in a UC Volume
VENDOR_CSV_PATH = "/Volumes/main/ai_parse_document_demo/ashwin_invoice_demo/Vend_ID_Numbers.csv"

# Target Delta table
VENDOR_TABLE = "main.ai_parse_document_demo.vendor_lookup"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load CSV and create Delta table

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("vendor_number", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("short_name", StringType(), True),
    StructField("company", StringType(), True),
    StructField("addr_1", StringType(), True),
    StructField("addr_2", StringType(), True),
    StructField("addr_3", StringType(), True),
    StructField("addr_4", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("pay_method", StringType(), True),
    StructField("tax_code", StringType(), True),
    StructField("terms", StringType(), True),
    StructField("routing", StringType(), True),
    StructField("category", StringType(), True),
    StructField("factor", StringType(), True),
])

df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("encoding", "UTF-8")
    .schema(schema)
    .load(VENDOR_CSV_PATH)
)

# Trim whitespace from all string columns
from pyspark.sql.functions import col, trim
for field in df.schema.fields:
    if isinstance(field.dataType, StringType):
        df = df.withColumn(field.name, trim(col(field.name)))

# Drop extra address columns not needed for matching
df = df.drop("addr_3", "addr_4")

print(f"Loaded {df.count()} vendor rows")
display(df.select("vendor_number", "name", "city", "state", "terms").orderBy("name").limit(10))

# COMMAND ----------

# Write to Delta table (overwrite to refresh)
df.write.format("delta").mode("overwrite").saveAsTable(VENDOR_TABLE)
print(f"Vendor table saved to {VENDOR_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_vendors FROM main.ai_parse_document_demo.vendor_lookup;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Spot check: show vendors with duplicate names (these need address disambiguation)
# MAGIC SELECT name, COUNT(*) AS cnt
# MAGIC FROM main.ai_parse_document_demo.vendor_lookup
# MAGIC GROUP BY name
# MAGIC HAVING COUNT(*) > 1
# MAGIC ORDER BY cnt DESC
