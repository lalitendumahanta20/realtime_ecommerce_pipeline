
import uuid
from datetime import datetime

from db_connection import get_connection


def start_pipeline_run():

    run_id = str(uuid.uuid4())
    batch_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            INSERT INTO dbo.pipeline_runs (
                run_id,
                batch_id,
                started_at,
                status,
                records_read,
                records_inserted,
                records_rejected
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            run_id,
            batch_id,
            datetime.now(),
            "RUNNING",
            0,
            0,
            0
        )

        conn.commit()

        return run_id

    except Exception:
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()


def complete_pipeline_run(
    run_id,
    records_read,
    records_inserted,
    records_rejected
):

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            UPDATE dbo.pipeline_runs
            SET
                completed_at = ?,
                status = ?,
                records_read = ?,
                records_inserted = ?,
                records_rejected = ?,
                error_message = NULL
            WHERE run_id = ?
            """,
            datetime.now(),
            "SUCCESS",
            records_read,
            records_inserted,
            records_rejected,
            run_id
        )

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()


def fail_pipeline_run(run_id, error_message):

    conn = get_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(
            """
            UPDATE dbo.pipeline_runs
            SET
                completed_at = ?,
                status = ?,
                error_message = ?
            WHERE run_id = ?
            """,
            datetime.now(),
            "FAILED",
            str(error_message)[:1000],
            run_id
        )

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()

