# Real-Time E-Commerce Event Processing Pipeline

A reliable, near-real-time e-commerce data pipeline built using **Python, Microsoft SQL Server, FastAPI, and Pytest**.

The project simulates an e-commerce event stream and processes incoming events in micro-batches. It focuses on data reliability, transaction safety, duplicate prevention, late-arriving events, data quality, monitoring, and reconciliation.

## Project Overview

E-commerce platforms generate events such as order creation, payment completion, order cancellation, and shipment updates.

This project demonstrates how to ingest, validate, process, store, and monitor these events while maintaining reliable processing state.

The pipeline supports:

- CSV-based event ingestion.
- Data validation and rejected-record quarantine.
- Micro-batch event processing.
- Idempotent processing using an event ledger.
- Source-offset checkpoint management.
- Event-time watermarking and late-event detection.
- Transaction commit and rollback.
- Historical aggregate backfilling.
- Source-to-aggregate reconciliation.
- Pipeline execution history and performance metrics.
- REST API endpoints using FastAPI.
- Automated data-quality checks and monitoring.

**Note:** This is a simulated near-real-time micro-batch pipeline, not a Kafka-based distributed streaming system.

## Architecture

```text
          E-Commerce Event Data
                    |
                    v
             CSV Event Source
                    |
                    v
           Python Ingestion Layer
                    |
          Validation and Deduplication
                    |
          +---------+----------+
          |                    |
          v                    v
     Valid Events         Rejected Events
          |                    |
          v                    v
     SQL Server           Quarantine Table
          |
          v
     Micro-Batch Processor
          |
     Event Ledger / Checkpoint
          |
     Watermark / Late Events
          |
          v
     Minute-Level Aggregates
          |
          v
      Reconciliation
          |
          v
    Pipeline Run History
          |
          v
    Monitoring and FastAPI
```

## Technology Stack

| Technology | Purpose |
|---|---|
| Python 3.11 | Pipeline implementation |
| Microsoft SQL Server | Persistent storage |
| SQL Server Management Studio | Database management |
| PyODBC | Python-SQL Server connectivity |
| FastAPI | REST API development |
| Pydantic | Request validation |
| Pandas | Data handling |
| Pytest | Automated testing |
| Python-dotenv | Environment configuration |
| CSV | Sample event source |

## Key Engineering Features

### 1. Reliable Data Ingestion

- Reads e-commerce events from CSV.
- Validates required fields, event types, timestamps, amounts, and source offsets.
- Prevents duplicate event ingestion.
- Stores rejected records separately for investigation.

### 2. Micro-Batch Processing

- Processes events in configurable batches.
- Maintains source offsets and checkpoints.
- Calculates event-time watermarks.
- Detects late-arriving events.
- Creates minute-level aggregates.

### 3. Idempotency and Exactly-Once Database Effects

Uses a processed-event ledger to prevent an event from affecting database aggregates more than once.

Duplicate events are skipped during replay, while processing checkpoints are maintained.

The event ledger and aggregate updates are handled transactionally.

### 4. Crash Recovery and Transaction Safety

- Uses database transactions.
- Commits successful batches.
- Rolls back failed batches.
- Supports retrying uncommitted events.
- Prevents checkpoint advancement when a batch transaction fails.

### 5. Data Quality Monitoring

Implements four automated checks:

- Missing events.
- Duplicate events.
- Invalid financial amounts.
- Aggregate mismatches.

The monitoring module generates JSON reports and appends them to a log file.

A Windows Task Scheduler configuration was also tested for periodic execution.

### 6. Pipeline Observability

Stores execution details in SQL Server, including:

- Run ID and execution status.
- Records read, inserted, and rejected.
- Batches processed.
- Duplicate events skipped.
- Late events detected.
- Final checkpoint and watermark.
- Processing duration and throughput.
- Total pipeline execution time.
- Error messages for failed runs.

### 7. REST API

FastAPI exposes endpoints for ingestion, health checks, data quality, and pipeline monitoring.

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/health` | Application health |
| GET | `/ready` | Readiness check |
| POST | `/api/v1/events` | Event batch ingestion |
| GET | `/quality/check` | Detailed quality report |
| GET | `/quality/summary` | Quality summary |
| GET | `/pipeline/runs` | Pipeline execution history |
| GET | `/pipeline/summary` | Pipeline execution statistics |

Interactive API documentation is available through `/docs` when the server is running.

## Database Design

The pipeline uses Microsoft SQL Server tables for event storage, processing state, aggregates, and monitoring.

Important tables include:

| Table | Purpose |
|---|---|
| `events` | Stores source events |
| `processed_events` | Tracks processed event IDs |
| `event_quarantine` | Stores rejected records |
| `pipeline_checkpoints` | Maintains source offsets |
| `pipeline_watermarks` | Maintains event-time watermarks |
| `minute_aggregates` | Stores aggregated event metrics |
| `pipeline_runs` | Stores execution history |

## Project Structure

```text
realtime_ecommerce_pipeline/
│
├── api/
│   ├── main.py
│   ├── schemas.py
│   └── __init__.py
│
├── data/
│   ├── sample_events.csv
│   └── sample_events_backup.csv
│
├── sql/
│   └── RealtimeEcommerceDB.sql
│
├── src/
│   ├── db_connection.py
│   ├── generate_events.py
│   ├── load_events.py
│   ├── process_batches.py
│   ├── reconcile.py
│   ├── backfill_aggregates.py
│   ├── run_history.py
│   ├── data_quality.py
│   └── monitor_quality.py
│
├── tests/
│   ├── conftest.py
│   ├── test_api_schemas.py
│   ├── test_backfill.py
│   ├── test_load_events.py
│   ├── test_process_batches.py
│   ├── test_reconciliation.py
│   └── test_transactions.py
│
├── main.py
├── config.py
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

## Installation and Setup

### Prerequisites

- Python 3.11
- Microsoft SQL Server
- SQL Server ODBC Driver 17 or 18
- Git
- SQL Server Management Studio (recommended)

### 1. Clone the repository

```bash
git clone https://github.com/lalitendumahanta20/realtime_ecommerce_pipeline.git
cd realtime_ecommerce_pipeline
```

### 2. Create a virtual environment

Windows:

```bash
python -m venv .venv
.venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment variables

Create your local `.env` file using the example:

```bash
copy .env.example .env
```

Configure your SQL Server connection in `.env`.

Example:

```env
DB_SERVER=localhost
DB_NAME=RealtimeEcommerceDB
DB_DRIVER=
DB_TRUSTED_CONNECTION=yes
DB_USERNAME=
DB_PASSWORD=

API_HOST=127.0.0.1
API_PORT=8000
API_KEY=
```

Use your own database connection settings. Do not commit `.env` or actual credentials to GitHub.

### 5. Initialize the database

Create the `RealtimeEcommerceDB` database in SQL Server and execute:

```text
sql/RealtimeEcommerceDB.sql
```

Verify that the required tables are created successfully.

### 6. Run the pipeline

```bash
python main.py
```

The pipeline executes ingestion, micro-batch processing, reconciliation, and execution-history recording.

### 7. Run the API

```bash
uvicorn api.main:app --reload
```

Open:

```text
http://127.0.0.1:8000/docs
```

### 8. Run data-quality monitoring

```bash
python -m src.monitor_quality
```

The monitoring report is appended to `logs/data_quality.log`.

### 9. Run automated tests

```bash
pytest -q
```

## Testing and Verification

The automated test suite covers:

- API request validation.
- Event ingestion and duplicate handling.
- Micro-batch processing.
- Checkpoint and watermark behavior.
- Transaction commit and rollback.
- Crash recovery behavior.
- Historical backfilling.
- Data reconciliation.

**Latest verified local test result: 51 passed, 1 warning.**

The pipeline's failure-injection test verifies rollback before commit and successful retry afterward.

## Reliability Design

An important design decision is using a transactional event ledger alongside aggregate updates.

This ensures that replaying an already processed event does not apply its database effects again.

The project demonstrates exactly-once database effects within its transactional processing workflow. It does not claim exactly-once delivery across external systems.

## Future Improvements

- Integrate Apache Kafka for distributed event streaming.
- Introduce Apache Spark for large-scale distributed processing.
- Containerize the application using Docker.
- Add CI/CD using GitHub Actions.
- Build a monitoring dashboard.
- Add automated alert delivery.
- Deploy the pipeline to a cloud environment.

## Author

**Lalitendu Mahanta**

GitHub: https://github.com/lalitendumahanta20
