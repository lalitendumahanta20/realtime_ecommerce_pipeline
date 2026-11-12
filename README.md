
# Real-Time E-Commerce Event Processing Pipeline

## Project Overview

This project implements a simulated real-time e-commerce event processing pipeline using Python and Microsoft SQL Server.

It processes incoming e-commerce events in micro-batches, validates records, removes duplicates, detects late-arriving events, maintains processing checkpoints, calculates minute-level aggregates, and reconciles processed data.

The project demonstrates reliable data ingestion, transaction management, fault handling, backfilling, and performance measurement.

## Technology Stack

- Python 3.11
- Microsoft SQL Server
- SQL Server Management Studio (SSMS)
- pyodbc
- pytest
- python-dotenv
- CSV

## Key Features

- Event generation with reproducible sample data.
- CSV-based event ingestion.
- Data validation and error quarantine.
- Duplicate event detection.
- Micro-batch processing.
- Configurable batch size.
- Event-time watermark calculation.
- Late-arriving event detection.
- Source offset and checkpoint management.
- Transaction handling with commit and rollback.
- Historical aggregate backfilling.
- Source-to-aggregate reconciliation.
- Pipeline execution history.
- Logging and performance metrics.
- Automated unit and integration-oriented tests.

## Project Structure

```text
realtime_ecommerce_pipeline/
│
├── data/
│   ├── sample_events.csv
│   └── sample_events_backup.csv
│
├── src/
│   ├── db_connection.py
│   ├── generate_events.py
│   ├── load_events.py
│   ├── process_batches.py
│   ├── reconcile.py
│   ├── backfill_aggregates.py
│   └── run_history.py
│
├── tests/
│   ├── conftest.py
│   ├── test_load_events.py
│   ├── test_process_batches.py
│   ├── test_transactions.py
│   ├── test_backfill.py
│   └── test_reconciliation.py
│
├── sql/
├── output/
├── logs/
├── main.py
├── config.py
├── requirements.txt
├── .env
├── .gitignore
└── README.md
```

## Database Tables

The pipeline uses the following SQL Server tables:

| Table | Purpose |
|---|---|
| events | Stores validated source events |
| event_quarantine | Stores rejected records |
| pipeline_checkpoints | Maintains source processing offsets |
| pipeline_runs | Stores pipeline execution history |
| minute_aggregates | Stores event-time aggregates |
| pipeline_watermarks | Stores watermark information |

## Installation and Setup

### 1. Clone the repository

```bash
git clone https://github.com/lalitendumahanta20/realtime_ecommerce_pipeline
cd realtime_ecommerce_pipeline
```

### 2. Create a virtual environment

```bash
python -m venv venv
```

Activate it on Windows:

```powershell
venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure SQL Server

Create a database named:

```text
RealtimeEcommerceDB
```

Execute the SQL scripts in the `sql/` directory to create the required tables and indexes.

Configure your database connection using the project's configuration and environment settings.

### 5. Generate sample events

```bash
python src/generate_events.py
```

### 6. Run the complete pipeline

```bash
python main.py
```

### 7. Run automated tests

```bash
pytest tests/ -v
```

## Performance Test Results

The pipeline was tested with 1,000 newly generated events.

| Metric | Result |
|---|---:|
| Events ingested | 1,000 |
| Events processed | 1,000 |
| Micro-batches | 100 |
| Batch size | 10 |
| Duplicates skipped | 0 |
| Records rejected | 0 |
| Late events detected | 499 |
| Ingestion duration | 3.7275 seconds |
| Processing duration | 4.9653 seconds |
| Processing throughput | 201.40 events/second |
| Reconciliation duration | 0.0537 seconds |
| Total execution time | 8.9474 seconds |
| Pipeline status | SUCCESS |

These results represent one local test run and are not a production performance guarantee.

## Data Quality and Reliability

The pipeline includes:

- Required-field validation.
- Event-type validation.
- Timestamp validation.
- Non-negative financial amount validation.
- Source offset validation.
- Duplicate event detection.
- Rejected-event quarantine.
- Transaction rollback on failures.
- Checkpoint-based restart behavior.
- Historical aggregate backfilling.
- Source-to-aggregate reconciliation.

## Testing

The automated test suite covers:

- Event validation.
- Duplicate handling.
- Invalid and missing values.
- Micro-batch processing.
- Watermark calculations.
- Late-event detection.
- Transaction commit and rollback.
- Retry behavior.
- Historical backfill.
- Data reconciliation.

**Latest recorded test result: 43 passed.**

## Future Improvements

- Integrate Apache Kafka for real-time event streaming.
- Add Apache Spark for distributed processing.
- Build a monitoring dashboard.
- Add configurable retry policies.
- Add automated performance benchmarking.
- Deploy the pipeline using Docker.

## Author
Lalitendu Mahanta

