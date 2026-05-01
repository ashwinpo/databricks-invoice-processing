# Databricks notebook source
# MAGIC %md
# MAGIC # Invoice Vendor Matching Pipeline
# MAGIC
# MAGIC This notebook takes parsed invoice PDFs from a UC Volume, extracts key fields,
# MAGIC and matches each invoice to the correct vendor using **Vector Search** for fast,
# MAGIC scalable similarity matching. It then calculates due dates from the vendor table's payment terms.
# MAGIC
# MAGIC **How it works:**
# MAGIC 1. **(One-time setup)** Create a Vector Search index on the vendor lookup table
# MAGIC 2. Parse PDFs with `ai_parse_document`
# MAGIC 3. Extract vendor name, address, invoice number, date, and total with `ai_query`
# MAGIC 4. Query the Vector Search index to find the best vendor match
# MAGIC 5. Score confidence based on match score + gap to next-best candidate
# MAGIC 6. Calculate due date from vendor table terms + invoice date
# MAGIC
# MAGIC **Why Vector Search instead of `ai_similarity`?**
# MAGIC `ai_similarity` requires a CROSS JOIN — with 16k vendors × N invoices that's 16k API calls
# MAGIC per invoice. Vector Search embeds the vendor table **once**, then each invoice lookup is a
# MAGIC single fast query against pre-computed embeddings.
# MAGIC
# MAGIC **What you need to configure (next cell):**
# MAGIC - Path to the UC Volume containing your invoice PDFs
# MAGIC - Catalog/schema for working tables
# MAGIC - Name of your vendor lookup table
# MAGIC - Vector Search endpoint name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC Edit these values to match your environment.

# COMMAND ----------

# Volume containing invoice PDFs
VOLUME_PATH = "/Volumes/main/ai_parse_document_demo/ashwin_invoice_demo"

# Catalog and schema for working tables
CATALOG = "main"
SCHEMA = "ai_parse_document_demo"

# Vendor lookup table (must already exist — load from CSV if needed)
VENDOR_TABLE = f"{CATALOG}.{SCHEMA}.vendor_lookup"

# Vector Search configuration
VS_ENDPOINT_NAME = "one-env-shared-endpoint-11"
VS_SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.vendor_vs_source"
VS_INDEX_NAME = f"{CATALOG}.{SCHEMA}.vendor_vs_index"
EMBEDDING_MODEL = "databricks-gte-large-en"

# Output table for matched results
RESULTS_TABLE = f"{CATALOG}.{SCHEMA}.invoice_vendor_matches"

# AI model for field extraction
AI_MODEL = "databricks-claude-sonnet-4-6"

# Confidence thresholds — tune these based on your results
# VS cosine scores tend to be slightly lower than ai_similarity scores
AUTO_MATCH_SCORE = 0.80       # Minimum score for auto-match
AUTO_MATCH_GAP = 0.10         # Minimum gap to #2 for auto-match
HIGH_CONF_SCORE = 0.90        # High score can have smaller gap
HIGH_CONF_GAP = 0.05          # Smaller gap OK if score is very high
REVIEW_THRESHOLD = 0.70       # Below this = MANUAL entry needed

# Number of candidate matches to retrieve per invoice
NUM_CANDIDATES = 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: One-time Vector Search setup
# MAGIC
# MAGIC This creates a source table with a combined `match_text` column and a Vector Search
# MAGIC index with managed embeddings. **Run this once, then skip on subsequent runs.**
# MAGIC
# MAGIC To refresh after vendor table changes, just re-run this cell — it recreates the source
# MAGIC table and triggers a re-sync of the index.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import time

w = WorkspaceClient()

# -- 1. Create the VS source table with a combined match_text column --
spark.sql(f"""
    CREATE OR REPLACE TABLE {VS_SOURCE_TABLE}
    TBLPROPERTIES (delta.enableChangeDataFeed = true)
    AS SELECT
        CAST(vendor_number AS STRING) AS vendor_id,
        vendor_number,
        name,
        addr_1,
        city,
        state,
        terms,
        category,
        routing,
        CONCAT(name, ', ', COALESCE(addr_1, ''), ', ', city, ' ', state) AS match_text
    FROM {VENDOR_TABLE}
""")

row_count = spark.sql(f"SELECT COUNT(*) FROM {VS_SOURCE_TABLE}").first()[0]
print(f"Vendor VS source table created with {row_count} rows")

# -- 2. Create or update the Vector Search index --
try:
    existing = w.vector_search_indexes.get_index(VS_INDEX_NAME)
    print(f"Index {VS_INDEX_NAME} already exists — triggering sync...")
    w.vector_search_indexes.sync_index(VS_INDEX_NAME)
except Exception:
    print(f"Creating new index {VS_INDEX_NAME}...")
    from databricks.sdk.service.vectorsearch import (
        DeltaSyncVectorIndexSpecRequest,
        EmbeddingSourceColumn,
    )
    w.vector_search_indexes.create_index(
        name=VS_INDEX_NAME,
        endpoint_name=VS_ENDPOINT_NAME,
        primary_key="vendor_id",
        index_type="DELTA_SYNC",
        delta_sync_index_spec=DeltaSyncVectorIndexSpecRequest(
            source_table=VS_SOURCE_TABLE,
            embedding_source_columns=[
                EmbeddingSourceColumn(
                    name="match_text",
                    embedding_model_endpoint_name=EMBEDDING_MODEL,
                )
            ],
            pipeline_type="TRIGGERED",
            columns_to_sync=[
                "vendor_id", "vendor_number", "name", "addr_1",
                "city", "state", "terms", "category", "routing", "match_text",
            ],
        ),
    )

# Wait for index to be ready
print("Waiting for index to be ready...")
for i in range(60):
    idx = w.vector_search_indexes.get_index(VS_INDEX_NAME)
    if idx.status.ready:
        print(f"Index ready — {idx.status.indexed_row_count} rows indexed")
        break
    time.sleep(5)
    if i % 6 == 0:
        print(f"  Still syncing... ({i*5}s)")
else:
    print("WARNING: Index not ready after 5 minutes — check status manually")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Parse invoices from volume with `ai_parse_document`

# COMMAND ----------

# Read all PDFs from the volume
pdf_df = (
    spark.read.format("binaryFile")
    .option("pathGlobFilter", "*.pdf")
    .load(VOLUME_PATH)
)

pdf_count = pdf_df.count()
print(f"Found {pdf_count} PDF(s) in {VOLUME_PATH}")
display(pdf_df.select("path", "length"))

# COMMAND ----------

# Parse documents and extract text content
# ai_parse_document returns a VARIANT — use variant_explode and :: cast syntax
content_df = spark.sql(f"""
    WITH parsed AS (
        SELECT
            path,
            ai_parse_document(content) AS parsed
        FROM read_files('{VOLUME_PATH}', format => 'binaryFile', pathGlobFilter => '*.pdf')
    ),
    exploded AS (
        SELECT
            path,
            elem:type::STRING AS elem_type,
            elem:content::STRING AS content
        FROM parsed,
        LATERAL VARIANT_EXPLODE(parsed:document:elements) AS (pos, key, elem)
    )
    SELECT
        path,
        concat_ws('\\n', collect_list(content)) AS full_text
    FROM exploded
    WHERE content IS NOT NULL
    AND elem_type IN ('text', 'title', 'section_header', 'table', 'page_header', 'page_footer')
    GROUP BY path
""")

content_df.cache()
print(f"Parsed {content_df.count()} document(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Extract invoice fields with `ai_query`

# COMMAND ----------

content_df.createOrReplaceTempView("content_view")

EXTRACTION_PROMPT = (
    "Extract these fields from the document text below. "
    "Return ONLY a valid JSON object with no other text, no markdown.\\n\\n"
    '{"vendor_name": "...", "vendor_address": "...", "vendor_city": "...", '
    '"vendor_state": "...", "invoice_number": "...", "invoice_date": "YYYY-MM-DD", '
    '"invoice_total": 0.00, "po_number": "..."}\\n\\n'
    "Rules:\\n"
    "- vendor_name: The company that issued/sent this invoice (not the bill-to/customer)\\n"
    "- vendor_address: The issuing vendor street address\\n"
    "- vendor_city/vendor_state: Vendor location\\n"
    "- invoice_date: Issue date in YYYY-MM-DD\\n"
    "- invoice_total: Total amount due as a number\\n"
    "- po_number: Purchase order number or null\\n"
    "- If a field is not found, use null\\n\\n"
    "Document text:\\n"
)

# ai_query returns STRUCT<result, errorMessage>
# The LLM may wrap JSON in ```json``` fences — we strip those in Step 3
extracted_df = spark.sql(f"""
    SELECT
        path,
        full_text,
        ai_query(
            '{AI_MODEL}',
            concat('{EXTRACTION_PROMPT}', full_text),
            failOnError => false
        ) AS ai_response
    FROM content_view
""")

extracted_df.createOrReplaceTempView("extracted_view")
display(spark.sql("SELECT path, ai_response.result, ai_response.errorMessage FROM extracted_view"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Parse extracted JSON

# COMMAND ----------

# Parse the JSON responses into structured columns
# Strip markdown code fences (```json ... ```) that the LLM may add
parsed_extractions = spark.sql("""
    WITH cleaned AS (
        SELECT
            path,
            regexp_replace(ai_response.result, '```json\\\\n?|```\\\\n?', '') AS clean_json,
            ai_response.errorMessage AS extraction_error
        FROM extracted_view
        WHERE ai_response.errorMessage IS NULL
    )
    SELECT
        path,
        get_json_object(clean_json, '$.vendor_name') AS vendor_name,
        get_json_object(clean_json, '$.vendor_address') AS vendor_address,
        get_json_object(clean_json, '$.vendor_city') AS vendor_city,
        get_json_object(clean_json, '$.vendor_state') AS vendor_state,
        get_json_object(clean_json, '$.invoice_number') AS invoice_number,
        get_json_object(clean_json, '$.invoice_date') AS invoice_date,
        CAST(get_json_object(clean_json, '$.invoice_total') AS DOUBLE) AS invoice_total,
        get_json_object(clean_json, '$.po_number') AS po_number,
        extraction_error
    FROM cleaned
""")

parsed_extractions.cache()
parsed_extractions.createOrReplaceTempView("invoice_extractions")
display(parsed_extractions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Vendor matching via Vector Search + confidence scoring

# COMMAND ----------

from pyspark.sql import Row
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

# Collect extracted invoices
invoices = parsed_extractions.select(
    "path", "vendor_name", "vendor_address", "vendor_city", "vendor_state",
    "invoice_number", "invoice_date", "invoice_total", "po_number"
).collect()

print(f"Matching {len(invoices)} invoice(s) against vendor index...")

# Query Vector Search for each invoice
results = []
for inv in invoices:
    # Build the query text — same format as the vendor match_text column
    query_text = ", ".join(filter(None, [
        inv.vendor_name,
        inv.vendor_address,
        f"{inv.vendor_city} {inv.vendor_state}" if inv.vendor_city else inv.vendor_state,
    ]))

    if not query_text.strip():
        continue

    vs_results = w.vector_search_indexes.query_index(
        index_name=VS_INDEX_NAME,
        columns=["vendor_id", "vendor_number", "name", "city", "state", "terms", "category", "routing"],
        query_text=query_text,
        num_results=NUM_CANDIDATES,
    )

    candidates = vs_results.result.data_array if vs_results.result else []
    if not candidates:
        continue

    # candidates[i] = [vendor_id, vendor_number, name, city, state, terms, category, routing, score]
    top = candidates[0]
    second = candidates[1] if len(candidates) > 1 else [None]*9
    top_score = float(top[-1])
    second_score = float(second[-1]) if second[-1] else 0.0
    gap = top_score - second_score

    if top_score >= HIGH_CONF_SCORE and gap >= HIGH_CONF_GAP:
        confidence = "AUTO"
    elif top_score >= AUTO_MATCH_SCORE and gap >= AUTO_MATCH_GAP:
        confidence = "AUTO"
    elif top_score >= REVIEW_THRESHOLD:
        confidence = "REVIEW"
    else:
        confidence = "MANUAL"

    # Calculate due date
    terms = top[5] or "NET30"
    terms_days = {"NET15": 15, "NET21": 21, "NET30": 30, "NET45": 45, "NET60": 60, "IMMED": 0}
    days = terms_days.get(terms, 30)

    results.append({
        "path": inv.path,
        "invoice_number": inv.invoice_number,
        "invoice_date": inv.invoice_date,
        "invoice_total": inv.invoice_total,
        "po_number": inv.po_number,
        "extracted_vendor_name": inv.vendor_name,
        "extracted_vendor_city": inv.vendor_city,
        "vendor_number": int(top[1]) if top[1] else None,
        "matched_vendor_name": top[2],
        "matched_city": top[3],
        "matched_state": top[4],
        "terms": terms,
        "category": top[6],
        "routing": top[7],
        "match_score": round(top_score, 4),
        "score_gap": round(gap, 4),
        "match_confidence": confidence,
        "terms_days": days,
        # Include top candidates for review
        "candidate_2_name": second[2] if second[2] else None,
        "candidate_2_score": round(float(second[-1]), 4) if second[-1] else None,
    })

# Convert to DataFrame
if results:
    matched_df = spark.createDataFrame([Row(**r) for r in results])
    # Add calculated due date
    matched_df = matched_df.selectExpr(
        "*",
        "DATE_ADD(TO_DATE(invoice_date), terms_days) AS calculated_due_date"
    ).drop("terms_days")
    display(matched_df.orderBy("match_confidence", "match_score"))
else:
    print("No matches found — check that invoices were extracted successfully")
    matched_df = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Show candidates for REVIEW cases
# MAGIC
# MAGIC For invoices flagged as `REVIEW`, display all candidate matches for human disambiguation.

# COMMAND ----------

review_results = []
for inv in invoices:
    query_text = ", ".join(filter(None, [
        inv.vendor_name,
        inv.vendor_address,
        f"{inv.vendor_city} {inv.vendor_state}" if inv.vendor_city else inv.vendor_state,
    ]))
    if not query_text.strip():
        continue

    vs_results = w.vector_search_indexes.query_index(
        index_name=VS_INDEX_NAME,
        columns=["vendor_number", "name", "city", "state", "terms"],
        query_text=query_text,
        num_results=NUM_CANDIDATES,
    )

    for row in (vs_results.result.data_array or []):
        review_results.append({
            "path": inv.path,
            "extracted_vendor": inv.vendor_name,
            "vendor_number": int(row[0]) if row[0] else None,
            "candidate_name": row[1],
            "candidate_city": row[2],
            "candidate_state": row[3],
            "terms": row[4],
            "match_score": round(float(row[-1]), 4),
        })

if review_results:
    candidates_df = spark.createDataFrame([Row(**r) for r in review_results])
    display(candidates_df.orderBy("path", candidates_df.match_score.desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Save results

# COMMAND ----------

if matched_df:
    matched_df.write.format("delta").mode("overwrite").saveAsTable(RESULTS_TABLE)
    print(f"Results saved to {RESULTS_TABLE}")
    display(spark.sql(f"SELECT * FROM {RESULTS_TABLE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Confidence Level | Meaning | Action |
# MAGIC |---|---|---|
# MAGIC | **AUTO** | High score + large gap to runner-up | No human review needed |
# MAGIC | **REVIEW** | Good score but close alternatives | Human picks from top candidates |
# MAGIC | **MANUAL** | Low score — vendor likely not in table | Human enters vendor number manually |
# MAGIC
# MAGIC ### Tuning Tips
# MAGIC - If too many invoices land in REVIEW, lower `AUTO_MATCH_GAP` (e.g., 0.08)
# MAGIC - If wrong vendors get AUTO matched, raise `AUTO_MATCH_SCORE` (e.g., 0.85)
# MAGIC - For vendors with many locations (e.g., What Chefs Want), the Ship To store address
# MAGIC   may be the only disambiguator — these will always need REVIEW
# MAGIC - To refresh the index after vendor table changes, re-run the Step 0 cell
