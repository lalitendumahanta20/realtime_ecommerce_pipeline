# Real-Time E-Commerce API & Data Engineering Pipeline

A fresher-friendly, locally runnable portfolio project that demonstrates how an application sends e-commerce events through a REST API into SQL Server, then processes them in micro-batches to create event-time aggregates.

## 1. What this project does

```text
Client / Postman / Python producer
             |
             v
     FastAPI REST endpoint
   schema validation + API key
             |
             v
      SQL Server: events
   event_id deduplication
             |
             v
    Existing batch processor
 offsets + watermark + late events
             |
             v
 minute_aggregates + checkpoints
             |
             v
      reconciliation + logs
```

The REST endpoint is an additional ingestion path. The existing CSV loader and micro-batch processor remain available. The API writes validated records into the same `dbo.events` table, so the existing processor can process API events on its next run.

## 2. Technology

- Python 3.10+ (project was originally developed with Python 3.11)
- FastAPI + Uvicorn for REST ingestion
- Pydantic for request validation
- Microsoft SQL Server + ODBC Driver 17/18
- pyodbc for database access
- pytest for automated tests
- python-dotenv for configuration

## 3. Project layout

```text
realtime_ecommerce_pipeline/
├── api/
│   ├── __init__.py
│   ├── main.py              # REST endpoints: health, readiness, ingest
│   └── schemas.py           # Pydantic request/response validation
├── data/                    # Sample CSV events
├── logs/                    # Runtime logs (created when needed)
├── output/
├── sql/
│   └── RealtimeEcommerceDB.sql
├── src/
│   ├── db_connection.py     # Environment-based SQL connection
│   ├── generate_events.py
│   ├── load_events.py       # CSV ingestion
│   ├── process_batches.py   # Micro-batches, offsets, watermark, aggregates
│   ├── reconcile.py
│   ├── backfill_aggregates.py
│   └── run_history.py
├── tests/
├── .env.example
├── config.py
├── main.py                  # Runs CSV -> process -> reconcile workflow
└── requirements.txt
```

## 4. Local setup (Windows)

### Step 1 — Install prerequisites

1. Install Python 3.10 or newer.
2. Install Microsoft SQL Server (Developer or Express) and SQL Server Management Studio (SSMS).
3. Install Microsoft ODBC Driver 17 or 18 for SQL Server.
4. In SSMS, confirm that your SQL Server instance is running and that you can connect to it.

### Step 2 — Open the project folder

Extract the ZIP. Open the `realtime_ecommerce_pipeline` folder in VS Code. In the VS Code terminal:

```powershell
python --version
python -m venv .venv
.venv\Scripts\activate
python -m pip install --upgrade pip
pip install -r requirements.txt
```

### Step 3 — Configure database connection

Copy `.env.example` to `.env` in the project root. Edit `DB_SERVER` to match your SQL Server instance:

- Default instance: `localhost` or `.`
- SQL Express: `localhost\\SQLEXPRESS`
- LocalDB: `(localdb)\\MSSQLLocalDB`
- Remote server: `HOSTNAME` or `IP,PORT` (only if network access is configured)

For Windows Authentication keep `DB_TRUSTED_CONNECTION=yes`. For SQL Authentication set it to `no` and provide `DB_USERNAME` and `DB_PASSWORD`. Never commit `.env` or real credentials.

### Step 4 — Create database and tables

1. Open `sql/RealtimeEcommerceDB.sql` in SSMS.
2. Execute the database/table creation section at the top. If the database or tables already exist, do not rerun the `CREATE` statements blindly.
3. Confirm these tables exist under `RealtimeEcommerceDB`: `events`, `event_quarantine`, `pipeline_checkpoints`, `pipeline_runs`, `minute_aggregates`, and `pipeline_watermarks`.

### Step 5 — Test the database connection

```powershell
python src/db_connection.py
```

Do not continue until this prints a successful connection. If it fails, check the SQL Server service, instance name in `.env`, ODBC driver installation, authentication mode, and firewall/network access.

## 5. Start and test the REST API

Start from the project root with the virtual environment active:

```powershell
uvicorn api.main:app --reload --host 127.0.0.1 --port 8000
```

Open these local URLs:

- API documentation (Swagger UI): http://127.0.0.1:8000/docs
- Liveness: http://127.0.0.1:8000/health
- Database readiness: http://127.0.0.1:8000/ready

### Send a test event from PowerShell

```powershell
$body = @{
  events = @(
    @{
      event_id = "api-event-0001"
      event_type = "order_created"
      order_id = "ORD-1001"
      customer_id = "CUST-101"
      amount = 1299.50
      event_time = "2026-11-16T10:15:00"
      source_offset = 900001
      batch_id = "api-demo-001"
    }
  )
} | ConvertTo-Json -Depth 5

Invoke-RestMethod -Uri "http://127.0.0.1:8000/api/v1/events" `
  -Method Post -ContentType "application/json" -Body $body
```

Expected shape of response:

```json
{
  "status": "accepted",
  "received": 1,
  "inserted": 1,
  "duplicates": 0
}
```

Send the same `event_id` again. It should be counted as a duplicate and not inserted twice. The API validates event types, required event ID, timestamps, positive source offsets, non-negative amounts, and rejects unknown request fields.

### Optional API key

Set a long random `API_KEY` value in `.env`, restart Uvicorn, and include this header on POST requests:

```text
X-API-Key: your-secret-value
```

Leave `API_KEY` blank for local development without authentication. Do not expose an unauthenticated development API publicly.

## 6. Run the downstream pipeline

After posting API events, stop the API only if you want to; both services can run simultaneously. In a second terminal, from the project root:

```powershell
.venv\Scripts\activate
python main.py
```

This existing workflow loads `data/sample_events.csv`, processes pending rows from SQL Server using micro-batches, updates checkpoints/watermarks, writes minute aggregates, and reconciles the output. Because the CSV loader runs as part of `main.py`, it will also load the sample CSV before processing API events.

Useful SQL checks in SSMS:

```sql
USE RealtimeEcommerceDB;
SELECT TOP 20 * FROM dbo.events ORDER BY ingestion_time DESC;
SELECT * FROM dbo.pipeline_checkpoints;
SELECT * FROM dbo.pipeline_watermarks;
SELECT * FROM dbo.minute_aggregates ORDER BY window_start DESC;
SELECT COUNT(*) AS total_events, COUNT(DISTINCT event_id) AS unique_events
FROM dbo.events;
```

## 7. Run tests

```powershell
pytest tests/ -v
```

The original tests cover the existing loader, batch processing, transactions, backfill, and reconciliation logic. API contract/manual verification is available through `/docs`. A passing unit test suite does not prove a live SQL Server connection; run the connection check and end-to-end steps separately.

## 8. Important design concepts to explain in an interview

- **API ingestion:** producers send JSON events over HTTP; FastAPI validates the request before writing to the database.
- **Idempotency:** `event_id` is the business-level unique identifier. Retrying the same request should not create a second event. The database primary key remains the final uniqueness guard.
- **Micro-batching:** the processor reads a bounded number of unprocessed events, aggregates them, and commits output and checkpoint updates transactionally.
- **Checkpoint/offset:** remembers the last processed source offset so the next run can resume rather than starting from the beginning.
- **Event time vs ingestion time:** event time says when the business event happened; ingestion time says when the pipeline received it.
- **Watermark / late events:** a watermark tracks event-time progress while allowing a configured lateness window; older events are identified as late.
- **Quarantine:** malformed CSV events are written to `event_quarantine` with the rejection reason rather than silently discarded.
- **Reconciliation:** compares processed aggregates with source data to identify missing or inconsistent processing.
- **Transactions:** commit successful batch work; rollback on failure to avoid partially persisted results.

## 9. Current scope and production gaps

This is a learning/portfolio implementation, not a production-ready distributed platform. The API currently inserts directly into SQL Server; it does not include Kafka, a managed queue, distributed workers, OAuth/JWT, rate limiting, schema registry, or cloud deployment. The current duplicate check plus SQL primary key protects event uniqueness, but concurrent duplicate requests can still race and return a database error; a production implementation should handle unique-key conflicts explicitly. Also review offset uniqueness and source partitioning before using multiple producers: the included processor assumes a single ordered source-offset stream.

Suggested next enhancements: asynchronous queue between API and database, structured JSON logs, retry/backoff for transient database errors, metrics dashboard, Docker-based app packaging, CI pipeline, and integration tests against a disposable SQL Server database.

## 10. Resume / interview project summary

**Real-Time E-Commerce API & Event Processing Pipeline** — Built a FastAPI-based ingestion service that validates JSON e-commerce events and writes them idempotently to SQL Server. Integrated the API with a Python micro-batch processing workflow featuring source-offset checkpoints, event-time watermarks, late-event detection, minute-level aggregations, transactional processing, and source-to-aggregate reconciliation. Added environment-based configuration, health/readiness endpoints, API documentation, and automated tests.

Only claim measured throughput, event counts, or percentage improvements after rerunning the project and recording your own results.
