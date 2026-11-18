"""FastAPI ingestion service. Run with: uvicorn api.main:app --reload"""
from datetime import datetime, timezone
from fastapi import FastAPI, Header, HTTPException, Depends, Query
from config import API_KEY
from src.db_connection import get_connection
from api.schemas import EventBatch, IngestResponse
from src.data_quality import run_quality_checks

app = FastAPI(
    title='Real-Time E-Commerce Event API',
    version='1.0.0',
    description='Validated, idempotent event ingestion API for the SQL Server pipeline.'
)


def authorize(x_api_key: str | None = Header(default=None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail='Invalid or missing API key')


@app.get('/health', tags=['Operations'])
def health():
    """Liveness check; does not require the database."""
    return {'status': 'ok', 'service': 'ecommerce-event-api'}


@app.get('/ready', tags=['Operations'])
def readiness():
    """Readiness check including SQL Server connectivity."""
    try:
        conn = get_connection()
        try:
            conn.cursor().execute('SELECT 1').fetchone()
        finally:
            conn.close()
        return {'status': 'ready', 'database': 'connected'}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f'Database unavailable: {exc}')


@app.get('/quality/check', tags=['Data Quality'])
def quality_check():
    """Run all data-quality checks and return detailed results."""
    try:
        return run_quality_checks()
    except Exception:
        raise HTTPException(
            status_code=503,
            detail='Data-quality checks unavailable'
        )


@app.get('/quality/summary', tags=['Data Quality'])
def quality_summary():
    """Return a compact data-quality monitoring summary."""
    try:
        report = run_quality_checks()
        
        passed_checks = sum(
            1 for check in report['checks']
            if check['status'] == 'PASS'
        )

        failed_checks = sum(
            1 for check in report['checks']
            if check['status'] == 'FAIL'
        )

        return {
            'overall_status': report['overall_status'],
            'checked_at': report['checked_at'],
            'total_checks': report['total_checks'],
            'passed_checks': passed_checks,
            'failed_checks': failed_checks,
            'total_issues': report['total_issues']
        }

    except Exception:
        raise HTTPException(
            status_code=503,
            detail='Data-quality summary unavailable'
        )


@app.get('/pipeline/runs', tags=['Pipeline Monitoring'])
def get_pipeline_runs(
    status: str | None = Query(default=None),
    limit: int = Query(default=20, ge=1, le=100)
):
    """Return recent pipeline runs with execution metrics."""

    if status is not None:
        status = status.upper()

        if status not in ('RUNNING', 'SUCCESS', 'FAILED'):
            raise HTTPException(
                status_code=400,
                detail='Status must be RUNNING, SUCCESS, or FAILED'
            )

    conn = get_connection()
    cursor = conn.cursor()

    try:
        query = """
            SELECT TOP (?)
                run_id,
                batch_id,
                started_at,
                completed_at,
                status,
                records_read,
                records_inserted,
                records_rejected,
                batches_processed,
                processing_events,
                duplicates_skipped,
                late_events_detected,
                final_checkpoint,
                final_watermark,
                processing_duration_seconds,
                events_per_second,
                ingestion_duration_seconds,
                reconciliation_duration_seconds,
                total_duration_seconds,
                error_message
            FROM dbo.pipeline_runs
        """

        params = [limit]

        if status:
            query += " WHERE status = ?"
            params.append(status)

        query += " ORDER BY started_at DESC"

        cursor.execute(query, *params)

        columns = [column[0] for column in cursor.description]

        runs = [
            dict(zip(columns, row))
            for row in cursor.fetchall()
        ]

        return {
            "count": len(runs),
            "runs": runs
        }

    except Exception:
        raise HTTPException(
            status_code=503,
            detail='Pipeline run history unavailable'
        )

    finally:
        cursor.close()
        conn.close()


@app.get('/pipeline/summary', tags=['Pipeline Monitoring'])
def get_pipeline_summary():
    """Return overall pipeline execution statistics."""

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute("""
            SELECT
                COUNT(*) AS total_runs,

                SUM(CASE
                    WHEN status = 'SUCCESS' THEN 1
                    ELSE 0
                END) AS successful_runs,

                SUM(CASE
                    WHEN status = 'FAILED' THEN 1
                    ELSE 0
                END) AS failed_runs,

                SUM(CASE
                    WHEN status = 'RUNNING' THEN 1
                    ELSE 0
                END) AS running_runs,

                AVG(
                    CASE
                        WHEN status = 'SUCCESS'
                        THEN total_duration_seconds
                    END
                ) AS average_duration_seconds

            FROM dbo.pipeline_runs
        """)

        row = cursor.fetchone()

        return {
            "total_runs": row[0] or 0,
            "successful_runs": row[1] or 0,
            "failed_runs": row[2] or 0,
            "running_runs": row[3] or 0,
            "average_duration_seconds": (
                float(row[4]) if row[4] is not None else 0.0
            )
        }

    except Exception:
        raise HTTPException(
            status_code=503,
            detail='Pipeline summary unavailable'
        )

    finally:
        cursor.close()
        conn.close()


@app.post('/api/v1/events', response_model=IngestResponse, status_code=202,
          dependencies=[Depends(authorize)], tags=['Ingestion'])
def ingest_events(batch: EventBatch):
    """Accept up to 1,000 validated events; event_id makes retries idempotent."""
    conn = get_connection()
    cursor = conn.cursor()
    inserted = duplicates = 0
    try:
        for event in batch.events:
            payload = event.model_dump()
            ingestion_time = payload['ingestion_time'] or datetime.now(timezone.utc)
            event_time = payload['event_time']
            # SQL Server datetime2 is timezone-naive; normalize aware inputs to UTC.
            if event_time.tzinfo is not None:
                event_time = event_time.astimezone(timezone.utc).replace(tzinfo=None)
            if ingestion_time.tzinfo is not None:
                ingestion_time = ingestion_time.astimezone(timezone.utc).replace(tzinfo=None)
            cursor.execute('SELECT 1 FROM dbo.events WHERE event_id = ?', payload['event_id'])
            if cursor.fetchone():
                duplicates += 1
                continue
            cursor.execute('''
                INSERT INTO dbo.events (
                    event_id, event_type, order_id, customer_id, amount,
                    event_time, ingestion_time, source_offset, batch_id, is_late
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            ''', payload['event_id'], payload['event_type'], payload['order_id'],
                 payload['customer_id'], payload['amount'], event_time,
                 ingestion_time, payload['source_offset'], payload['batch_id'])
            inserted += 1
        conn.commit()
        return IngestResponse(status='accepted', received=len(batch.events),
                              inserted=inserted, duplicates=duplicates)
    except Exception:
        conn.rollback()
        raise HTTPException(status_code=500, detail='Event batch failed; transaction rolled back')
    finally:
        cursor.close()
        conn.close()
