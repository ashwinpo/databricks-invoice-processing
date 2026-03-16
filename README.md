# Invoice Processing App

AI-powered invoice field extraction with human-in-the-loop review, built as a Databricks App.

Upload invoices, extract fields automatically using `ai_parse_document` + `ai_query`, review and correct in a split-pane UI, and save confirmed results to Delta Lake. Export to Excel or CSV.

---

## How It Works

```
Upload PDF --> ai_parse_document (layout + bounding boxes)
          --> ai_query (structured field extraction via LLM)
          --> Human review (PDF on left, editable fields on right)
          --> Confirm --> Delta Lake
```

1. **Upload**: Drag-drop one or more invoice PDFs. Duplicate filenames are auto-incremented (`invoice.pdf`, `invoice_1.pdf`, ...).
2. **Parse**: `ai_parse_document` extracts layout elements (text, tables, headers) with bounding box coordinates and page images.
3. **Extract**: `ai_query` sends concatenated document text to an LLM with field-specific prompts to extract structured data.
4. **Review**: Split-pane view -- PDF with bounding box overlay on the left, editable form on the right. Fix any errors.
5. **Confirm**: Save confirmed fields to Delta Lake. Auto-advances to the next unreviewed invoice.
6. **Export**: Download all results as formatted Excel or CSV from the queue view.

---

## Customizing Fields

Edit `backend/invoice_fields.yaml` to change what gets extracted. This is the single source of truth -- the backend reads it at startup and the frontend fetches it via API.

```yaml
fields:
  - name: invoice_number        # Key used in JSON and Delta table
    label: Invoice Number       # Display label in the UI form
    description: The unique...  # Used in the LLM prompt -- be specific
    type: string                # string | date | currency (affects UI input type)
```

**Tips for better extraction accuracy:**
- Write descriptions as if explaining to a human reviewer what to look for
- Be specific about disambiguation (e.g., "the date the invoice was issued, not the due date")
- The description is injected directly into the AI prompt, so more context = better results

To hot-reload fields without restarting: `POST /api/reload-invoice-fields`

---

## Deployment

### Prerequisites

- Databricks workspace with AI Functions enabled
- Unity Catalog with a catalog, schema, and volume created
- SQL Warehouse (Serverless recommended)
- A Foundation Model serving endpoint (default: `databricks-claude-sonnet-4-6`)
- Databricks CLI configured with a profile

### 1. Create the Databricks App

```bash
databricks apps create invoice-processing --profile YOUR_PROFILE
```

Grant the app's service principal access to:
- The Unity Catalog schema (read/write for tables and volumes)
- The SQL Warehouse
- The Foundation Model serving endpoint

### 2. Configure `backend/app.yaml`

```yaml
command: [uvicorn, app:app, --host, 0.0.0.0, --port, '8000']
env:
  - name: DATABRICKS_WAREHOUSE_ID
    value: your-warehouse-id            # SQL Warehouse ID
  - name: DATABRICKS_VOLUME_PATH
    value: /Volumes/catalog/schema/vol/ # Where uploaded PDFs are stored
  - name: DATABRICKS_DELTA_TABLE_PATH
    value: catalog.schema.parsed_docs   # ai_parse_document output table
  - name: INVOICE_RESULTS_TABLE
    value: catalog.schema.invoice_results  # Confirmed invoice fields
  - name: AI_QUERY_MODEL
    value: databricks-claude-sonnet-4-6 # Foundation Model endpoint name
  - name: STATIC_FILES_PATH
    value: /Workspace/Users/you@company.com/apps/invoice-app/static
runtime: python_3.10
```

### 3. Deploy

```bash
./deploy.sh "/Workspace/Users/you@company.com/apps/invoice-app" "invoice-processing" "YOUR_PROFILE"
```

The script builds the frontend, uploads everything to Databricks, and deploys the app.

---

## Local Development

```bash
# Backend
cd backend
pip install -r requirements.txt
uvicorn app:app --reload --port 8000

# Frontend (separate terminal)
cd frontend
npm install
npm run dev   # http://localhost:3000
```

Set `NEXT_PUBLIC_API_URL=http://localhost:8000` in `frontend/.env.local` for local dev.

---

## Project Structure

```
backend/
  app.py                 # FastAPI application -- all API endpoints
  app.yaml               # Databricks App configuration (env vars)
  invoice_fields.yaml    # Field definitions (what to extract)
  requirements.txt       # Python dependencies

frontend/
  src/app/
    page.tsx                        # Landing page
    document-intelligence/page.tsx  # Main app (queue + review views)
  src/lib/
    api-config.ts                   # API client with base URL detection
  src/components/ui/                # Shared UI components (button, card)

deploy.sh                # Build + upload + deploy script
```

## API Endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `/api/invoice-fields` | GET | Returns field config from YAML |
| `/api/reload-invoice-fields` | POST | Hot-reload field config |
| `/api/upload-to-uc` | POST | Upload PDFs to UC Volume (handles duplicates) |
| `/api/write-to-delta-table` | POST | Run ai_parse_document on uploaded files |
| `/api/query-delta-table` | POST | Query parsed document elements |
| `/api/page-metadata` | POST | Get page count and element counts |
| `/api/visualize-page` | POST | Get page image with bounding box overlay |
| `/api/extract-fields` | POST | Run ai_query to extract structured fields |
| `/api/invoice-queue` | GET | List all invoices with status |
| `/api/confirm-invoice` | POST | Save confirmed fields to Delta |
| `/api/export-invoices?format=csv\|xlsx` | GET | Export results as CSV or Excel |
| `/api/warehouse-config` | GET/POST | View/update warehouse ID |
| `/api/volume-path-config` | GET/POST | View/update volume path |
| `/api/delta-table-path-config` | GET/POST | View/update delta table path |

## Data Tables

**Parsed documents table** (`DATABRICKS_DELTA_TABLE_PATH`) -- created by `ai_parse_document`:
```
path STRING, element_id STRING, type STRING, bbox ARRAY<DOUBLE>,
page_id INT, content STRING, description STRING, image_uri STRING
```

**Invoice results table** (`INVOICE_RESULTS_TABLE`) -- created automatically:
```
file_path STRING, filename STRING, status STRING,
extracted_fields STRING (JSON), confirmed_fields STRING (JSON),
uploaded_at TIMESTAMP, confirmed_at TIMESTAMP
```

Field values are stored as JSON strings so the schema doesn't change when you add/remove fields in `invoice_fields.yaml`.
