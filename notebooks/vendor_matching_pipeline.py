# Databricks notebook source
# MAGIC %md
# MAGIC # Invoice Vendor Matching Pipeline
# MAGIC
# MAGIC This notebook takes parsed invoice PDFs from a UC Volume, extracts key fields,
# MAGIC and matches each invoice to the correct vendor in your vendor lookup table using
# MAGIC `ai_similarity`. It then calculates due dates from the vendor table's payment terms.
# MAGIC
# MAGIC **How it works:**
# MAGIC 1. Parse PDFs with `ai_parse_document`
# MAGIC 2. Extract vendor name, address, invoice number, date, and total with `ai_query`
# MAGIC 3. Match to vendor lookup table using `ai_similarity` on name + address
# MAGIC 4. Score confidence based on match score + gap to next-best candidate
# MAGIC 5. Calculate due date from vendor table terms + invoice date
# MAGIC
# MAGIC **What you need to configure (next cell):**
# MAGIC - Path to the UC Volume containing your invoice PDFs
# MAGIC - Catalog/schema for working tables
# MAGIC - Name of your vendor lookup table

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

# Vendor lookup table (must already exist — load from CSV first if needed)
VENDOR_TABLE = f"{CATALOG}.{SCHEMA}.vendor_lookup"

# Output table for matched results
RESULTS_TABLE = f"{CATALOG}.{SCHEMA}.invoice_vendor_matches"

# AI model for field extraction
AI_MODEL = "databricks-claude-sonnet-4-6"

# Confidence thresholds — tune these based on your results
AUTO_MATCH_SCORE = 0.85      # Minimum score for auto-match
AUTO_MATCH_GAP = 0.10        # Minimum gap to #2 for auto-match
HIGH_CONF_SCORE = 0.95       # High score can have smaller gap
HIGH_CONF_GAP = 0.05         # Smaller gap OK if score is very high
REVIEW_THRESHOLD = 0.80      # Below this = MANUAL entry needed

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 0: Verify vendor lookup table exists

# COMMAND ----------

vendor_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {VENDOR_TABLE}").first()["cnt"]
print(f"Vendor lookup table has {vendor_count} rows")
display(spark.sql(f"SELECT vendor_number, name, city, state, terms FROM {VENDOR_TABLE} ORDER BY name LIMIT 10"))

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

# ai_query returns STRUCT<result, errorMessage> (not response/error)
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
# MAGIC ## Step 3: Parse extracted JSON and match to vendor table

# COMMAND ----------

# Parse the JSON responses into structured columns
# ai_query returns STRUCT<result STRING, errorMessage STRING>
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

parsed_extractions.createOrReplaceTempView("invoice_extractions")
display(parsed_extractions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Vendor matching with `ai_similarity` + confidence scoring

# COMMAND ----------

matched_df = spark.sql(f"""
    WITH scored AS (
        SELECT
            i.path,
            i.invoice_number,
            i.invoice_date,
            i.invoice_total,
            i.po_number,
            i.vendor_name AS extracted_vendor_name,
            i.vendor_city AS extracted_vendor_city,
            i.vendor_state AS extracted_vendor_state,
            v.vendor_number,
            v.name AS matched_vendor_name,
            v.city AS matched_city,
            v.state AS matched_state,
            v.terms,
            v.category,
            v.routing,
            ai_similarity(
                concat(i.vendor_name, ', ', coalesce(i.vendor_address, ''), ', ', coalesce(i.vendor_city, ''), ' ', coalesce(i.vendor_state, '')),
                concat(v.name, ', ', v.addr_1, ', ', v.city, ' ', v.state)
            ) AS match_score,
            ROW_NUMBER() OVER (
                PARTITION BY i.path
                ORDER BY ai_similarity(
                    concat(i.vendor_name, ', ', coalesce(i.vendor_address, ''), ', ', coalesce(i.vendor_city, ''), ' ', coalesce(i.vendor_state, '')),
                    concat(v.name, ', ', v.addr_1, ', ', v.city, ' ', v.state)
                ) DESC
            ) AS rank
        FROM invoice_extractions i
        CROSS JOIN {VENDOR_TABLE} v
    ),
    -- Get top 2 matches per invoice to compute the gap
    top_matches AS (
        SELECT
            path,
            MAX(CASE WHEN rank = 1 THEN match_score END) AS top_score,
            MAX(CASE WHEN rank = 2 THEN match_score END) AS second_score,
            MAX(CASE WHEN rank = 1 THEN vendor_number END) AS vendor_number,
            MAX(CASE WHEN rank = 1 THEN matched_vendor_name END) AS matched_vendor_name,
            MAX(CASE WHEN rank = 1 THEN matched_city END) AS matched_city,
            MAX(CASE WHEN rank = 1 THEN matched_state END) AS matched_state,
            MAX(CASE WHEN rank = 1 THEN terms END) AS terms,
            MAX(CASE WHEN rank = 1 THEN category END) AS category,
            MAX(CASE WHEN rank = 1 THEN routing END) AS routing,
            MAX(CASE WHEN rank = 1 THEN invoice_number END) AS invoice_number,
            MAX(CASE WHEN rank = 1 THEN invoice_date END) AS invoice_date,
            MAX(CASE WHEN rank = 1 THEN invoice_total END) AS invoice_total,
            MAX(CASE WHEN rank = 1 THEN po_number END) AS po_number,
            MAX(CASE WHEN rank = 1 THEN extracted_vendor_name END) AS extracted_vendor_name,
            MAX(CASE WHEN rank = 1 THEN extracted_vendor_city END) AS extracted_vendor_city
        FROM scored
        WHERE rank <= 2
        GROUP BY path
    )
    SELECT
        path,
        invoice_number,
        invoice_date,
        invoice_total,
        po_number,
        extracted_vendor_name,
        extracted_vendor_city,
        vendor_number,
        matched_vendor_name,
        matched_city,
        matched_state,
        terms,
        category,
        routing,
        ROUND(top_score, 4) AS match_score,
        ROUND(top_score - second_score, 4) AS score_gap,
        CASE
            WHEN top_score >= {HIGH_CONF_SCORE} AND (top_score - second_score) >= {HIGH_CONF_GAP} THEN 'AUTO'
            WHEN top_score >= {AUTO_MATCH_SCORE} AND (top_score - second_score) >= {AUTO_MATCH_GAP} THEN 'AUTO'
            WHEN top_score >= {REVIEW_THRESHOLD} THEN 'REVIEW'
            ELSE 'MANUAL'
        END AS match_confidence,
        -- Calculate due date from vendor table terms + invoice date
        DATE_ADD(TO_DATE(invoice_date),
            CASE terms
                WHEN 'NET15' THEN 15
                WHEN 'NET21' THEN 21
                WHEN 'NET30' THEN 30
                WHEN 'NET45' THEN 45
                WHEN 'NET60' THEN 60
                WHEN 'IMMED' THEN 0
                ELSE 30
            END
        ) AS calculated_due_date
    FROM top_matches
    ORDER BY match_confidence, match_score DESC
""")

display(matched_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Show candidates for REVIEW cases
# MAGIC
# MAGIC For invoices flagged as `REVIEW`, display the top 5 vendor candidates so a human can pick the right one.

# COMMAND ----------

# Show top-5 candidates per invoice for disambiguation
candidates_df = spark.sql(f"""
    WITH scored AS (
        SELECT
            i.path,
            i.vendor_name AS extracted_vendor,
            v.vendor_number,
            v.name AS candidate_name,
            v.city AS candidate_city,
            v.state AS candidate_state,
            v.terms,
            ai_similarity(
                concat(i.vendor_name, ', ', coalesce(i.vendor_address, ''), ', ', coalesce(i.vendor_city, ''), ' ', coalesce(i.vendor_state, '')),
                concat(v.name, ', ', v.addr_1, ', ', v.city, ' ', v.state)
            ) AS score,
            ROW_NUMBER() OVER (
                PARTITION BY i.path
                ORDER BY ai_similarity(
                    concat(i.vendor_name, ', ', coalesce(i.vendor_address, ''), ', ', coalesce(i.vendor_city, ''), ' ', coalesce(i.vendor_state, '')),
                    concat(v.name, ', ', v.addr_1, ', ', v.city, ' ', v.state)
                ) DESC
            ) AS rank
        FROM invoice_extractions i
        CROSS JOIN {VENDOR_TABLE} v
    )
    SELECT
        path, extracted_vendor,
        vendor_number, candidate_name, candidate_city, candidate_state, terms,
        ROUND(score, 4) AS match_score
    FROM scored
    WHERE rank <= 5
    ORDER BY path, score DESC
""")

display(candidates_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Save results
# MAGIC
# MAGIC Save the matched results to a Delta table for downstream consumption.

# COMMAND ----------

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
# MAGIC - If wrong vendors get AUTO matched, raise `AUTO_MATCH_SCORE` (e.g., 0.90)
# MAGIC - For vendors with many locations (e.g., What Chefs Want), the Ship To store address
# MAGIC   may be the only disambiguator — these will always need REVIEW
