
import os
import time
from collections import defaultdict
from datetime import timedelta
from decimal import Decimal

from src.db_connection import get_connection


BATCH_SIZE = 10
SOURCE_NAME = "ecommerce_events"
ALLOWED_LATENESS_MINUTES = 5


def run_batch_transaction(conn, operation):
    try:
        result = operation()

        # Simulate a crash immediately before commit.
        if os.getenv("SIMULATE_BATCH_FAILURE") == "1":
            raise RuntimeError("SIMULATED_FAILURE_BEFORE_COMMIT")

        conn.commit()
        return result

    except Exception:
        conn.rollback()
        raise


def calculate_watermark(
    old_max_event_time,
    batch_max_event_time,
    allowed_lateness_minutes
):
    """Calculate a watermark without allowing it to move backwards."""

    if old_max_event_time is None:
        max_event_time = batch_max_event_time
    else:
        max_event_time = max(
            old_max_event_time,
            batch_max_event_time
        )

    watermark_time = max_event_time - timedelta(
        minutes=allowed_lateness_minutes
    )

    return max_event_time, watermark_time


def is_late_event(event_time, watermark_time):
    """Return True when an event is older than the watermark."""
    return event_time < watermark_time


def aggregate_events(rows):
    """Group events by event-time minute and event type."""

    aggregates = defaultdict(
        lambda: {
            "count": 0,
            "amount": Decimal("0.00")
        }
    )

    for row in rows:
        window_start = row.event_time.replace(
            second=0,
            microsecond=0
        )

        key = (window_start, row.event_type)

        aggregates[key]["count"] += 1

        if row.amount is not None:
            aggregates[key]["amount"] += Decimal(
                str(row.amount)
            )

    return aggregates


def fetch_next_batch(cursor, last_offset):
    """Fetch events after the last committed checkpoint."""

    cursor.execute(
        f"""
        SELECT TOP ({BATCH_SIZE})
            event_id,
            event_type,
            amount,
            event_time,
            source_offset
        FROM dbo.events
        WHERE source_offset > ?
        ORDER BY source_offset
        """,
        last_offset
    )

    return cursor.fetchall()


def get_last_offset(cursor):
    cursor.execute(
        """
        SELECT last_offset
        FROM dbo.pipeline_checkpoints
        WHERE source_name = ?
        """,
        SOURCE_NAME
    )

    result = cursor.fetchone()

    if result is None:
        return 0

    return result[0]


def get_watermark_details(cursor):
    cursor.execute(
        """
        SELECT
            max_event_time,
            allowed_lateness_minutes
        FROM dbo.pipeline_watermarks
        WHERE source_name = ?
        """,
        SOURCE_NAME
    )

    return cursor.fetchone()


def save_watermark(
    cursor,
    max_event_time,
    watermark_time,
    allowed_lateness
):
    cursor.execute(
        """
        IF EXISTS (
            SELECT 1
            FROM dbo.pipeline_watermarks
            WHERE source_name = ?
        )
        BEGIN
            UPDATE dbo.pipeline_watermarks
            SET
                max_event_time = ?,
                watermark_time = ?,
                updated_at = SYSUTCDATETIME()
            WHERE source_name = ?
        END
        ELSE
        BEGIN
            INSERT INTO dbo.pipeline_watermarks (
                source_name,
                max_event_time,
                watermark_time,
                allowed_lateness_minutes
            )
            VALUES (?, ?, ?, ?)
        END
        """,
        SOURCE_NAME,
        max_event_time,
        watermark_time,
        SOURCE_NAME,
        SOURCE_NAME,
        max_event_time,
        watermark_time,
        allowed_lateness
    )


def update_event_lateness(cursor, rows, watermark_time):
    late_count = 0

    for row in rows:
        late = is_late_event(
            row.event_time,
            watermark_time
        )

        cursor.execute(
            """
            UPDATE dbo.events
            SET is_late = ?
            WHERE event_id = ?
            """,
            int(late),
            row.event_id
        )

        if late:
            late_count += 1

    return late_count


def save_aggregates(cursor, aggregates):
    for (window_start, event_type), values in aggregates.items():

        cursor.execute(
            """
            UPDATE dbo.minute_aggregates
            SET
                event_count = event_count + ?,
                total_amount = total_amount + ?,
                updated_at = SYSUTCDATETIME()
            WHERE window_start = ?
              AND event_type = ?
            """,
            values["count"],
            values["amount"],
            window_start,
            event_type
        )

        if cursor.rowcount == 0:
            cursor.execute(
                """
                INSERT INTO dbo.minute_aggregates (
                    window_start,
                    event_type,
                    event_count,
                    total_amount
                )
                VALUES (?, ?, ?, ?)
                """,
                window_start,
                event_type,
                values["count"],
                values["amount"]
            )


def rebuild_minute_aggregates(cursor):
    """
    Rebuild all minute aggregates from the events source table.

    DELETE and INSERT must run inside the same transaction.
    Re-running this operation will not double-count events.
    """

    # Remove existing aggregate results.
    cursor.execute(
        """
        DELETE FROM dbo.minute_aggregates
        """
    )

    # Recalculate aggregates from the original events.
    cursor.execute(
        """
        INSERT INTO dbo.minute_aggregates (
            window_start,
            event_type,
            event_count,
            total_amount
        )
        SELECT
            DATEADD(
                MINUTE,
                DATEDIFF(MINUTE, 0, event_time),
                0
            ) AS window_start,
            event_type,
            COUNT(*) AS event_count,
            SUM(COALESCE(amount, 0)) AS total_amount
        FROM dbo.events
        GROUP BY
            DATEADD(
                MINUTE,
                DATEDIFF(MINUTE, 0, event_time),
                0
            ),
            event_type
        """
    )

    return cursor.rowcount


def backfill_aggregates():
    """
    Rebuild aggregate results without changing the checkpoint
    or watermark.
    """

    conn = get_connection()
    cursor = conn.cursor()

    try:
        result = run_batch_transaction(
            conn,
            lambda: rebuild_minute_aggregates(cursor)
        )

        print("Historical backfill completed successfully!")
        print(f"Aggregate groups inserted: {result}")

    except Exception as error:
        print("Historical backfill failed:", error)
        raise

    finally:
        cursor.close()
        conn.close()


def advance_checkpoint(cursor, new_offset):
    cursor.execute(
        """
        UPDATE dbo.pipeline_checkpoints
        SET
            last_offset = ?,
            updated_at = SYSUTCDATETIME()
        WHERE source_name = ?
        """,
        new_offset,
        SOURCE_NAME
    )


def claim_new_events(cursor, rows):
    """
    Register events in the processing ledger.
    Return only events that have not been processed before.
    """

    new_rows = []

    for row in rows:
        cursor.execute(
            """
            SET NOCOUNT ON;

            DECLARE @inserted TABLE (
                event_id VARCHAR(100) NOT NULL
            );

            INSERT INTO dbo.processed_events (
                event_id,
                source_offset
            )
            OUTPUT inserted.event_id
            INTO @inserted(event_id)
            SELECT ?, ?
            WHERE NOT EXISTS (
                SELECT 1
                FROM dbo.processed_events WITH (UPDLOCK, HOLDLOCK)
                WHERE event_id = ?
            );

            SELECT COUNT(*) AS inserted_count
            FROM @inserted;
            """,
            row.event_id,
            row.source_offset,
            row.event_id
        )

        result = cursor.fetchone()

        if result and result[0] == 1:
            new_rows.append(row)

    return new_rows


def process_one_batch(cursor, rows):
    """
    Process a batch with event-level idempotency.
    """

    # STEP 1: Claim only previously unprocessed events.
    new_rows = claim_new_events(cursor, rows)

    # Always advance checkpoint across fetched rows.
    new_offset = max(
        row.source_offset for row in rows
    )

    batch_late = 0

    # STEP 2: Process only newly claimed events.
    if new_rows:

        # Calculate watermark.
        watermark_row = get_watermark_details(cursor)

        batch_max_event_time = max(
            row.event_time for row in new_rows
        )

        if watermark_row:
            old_max_event_time = watermark_row.max_event_time
            allowed_lateness = (
                watermark_row.allowed_lateness_minutes
            )
        else:
            old_max_event_time = None
            allowed_lateness = ALLOWED_LATENESS_MINUTES

        max_event_time, watermark_time = calculate_watermark(
            old_max_event_time,
            batch_max_event_time,
            allowed_lateness
        )

        save_watermark(
            cursor,
            max_event_time,
            watermark_time,
            allowed_lateness
        )

        # STEP 3: Classify newly claimed events.
        batch_late = update_event_lateness(
            cursor,
            new_rows,
            watermark_time
        )

        # STEP 4: Aggregate only newly claimed events.
        aggregates = aggregate_events(new_rows)

        save_aggregates(cursor, aggregates)

    # STEP 5: Advance checkpoint.
    # This also handles batches containing only duplicates.
    advance_checkpoint(cursor, new_offset)

    return new_offset, batch_late, len(new_rows)


def process_batches():

    start_time = time.perf_counter()

    conn = get_connection()
    cursor = conn.cursor()

    total_processed = 0
    total_late = 0
    total_duplicates_skipped = 0
    batch_number = 0

    try:
        # Create checkpoint if it does not exist.
        cursor.execute(
            """
            IF NOT EXISTS (
                SELECT 1
                FROM dbo.pipeline_checkpoints
                WHERE source_name = ?
            )
            BEGIN
                INSERT INTO dbo.pipeline_checkpoints (
                    source_name,
                    last_offset
                )
                VALUES (?, 0)
            END
            """,
            SOURCE_NAME,
            SOURCE_NAME
        )

        conn.commit()

        print("Starting micro-batch processing...")

        while True:

            # Read the last committed checkpoint.
            last_offset = get_last_offset(cursor)

            # Fetch only new events.
            rows = fetch_next_batch(cursor, last_offset)

            # No new events: stop processing.
            if not rows:
                break

            batch_number += 1

            # Execute the complete batch inside one transaction.
            new_offset, batch_late, newly_processed = run_batch_transaction(
                conn,
                lambda: process_one_batch(cursor, rows)
            )

            duplicates_skipped = len(rows) - newly_processed

            total_processed += newly_processed
            total_late += batch_late
            total_duplicates_skipped += duplicates_skipped

            print(
                f"Batch {batch_number}: "
                f"{len(rows)} events fetched | "
                f"{newly_processed} new events processed | "
                f"{duplicates_skipped} duplicates skipped | "
                f"Late events: {batch_late} | "
                f"Checkpoint: {new_offset}"
            )

        # Get final checkpoint.
        final_offset = get_last_offset(cursor)

        # Get final watermark.
        cursor.execute(
            """
            SELECT watermark_time
            FROM dbo.pipeline_watermarks
            WHERE source_name = ?
            """,
            SOURCE_NAME
        )

        watermark_result = cursor.fetchone()

        final_watermark = (
            watermark_result[0] if watermark_result else None
        )

        # Calculate performance metrics.
        processing_duration = time.perf_counter() - start_time

        if processing_duration > 0:
            events_per_second = (
                total_processed / processing_duration
            )
        else:
            events_per_second = 0

        # Prepare processing metrics.
        metrics = {
            "batches_processed": batch_number,
            "events_processed": total_processed,
            "duplicates_skipped": total_duplicates_skipped,
            "late_events_detected": total_late,
            "final_checkpoint": final_offset,
            "final_watermark": final_watermark,
            "processing_duration_seconds": round(
                processing_duration, 4
            ),
            "events_per_second": round(events_per_second, 2)
        }

        # Print processing summary.
        print("\n========== PROCESSING SUMMARY ==========")
        print(f"Total batches processed: {batch_number}")
        print(f"Total events processed: {total_processed}")
        print(f"Total duplicates skipped: {total_duplicates_skipped}")
        print(f"Late events detected: {total_late}")
        print(f"Final checkpoint: {final_offset}")
        print(f"Final watermark: {final_watermark}")
        print(
            f"Processing duration: "
            f"{processing_duration:.4f} seconds"
        )
        print(
            f"Processing throughput: "
            f"{events_per_second:.2f} events/second"
        )

        print("Micro-batch processing completed!")

        return metrics

    except Exception as error:
        conn.rollback()
        print("Processing failed:", error)
        raise

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    process_batches()
