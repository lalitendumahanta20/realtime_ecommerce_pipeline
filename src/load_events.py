
import csv
import json
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

from db_connection import get_connection


PROJECT_DIR = Path(__file__).resolve().parent.parent
CSV_FILE = PROJECT_DIR / "data" / "sample_events.csv"

VALID_EVENT_TYPES = {
    "order_created",
    "payment_completed",
    "order_cancelled",
    "order_shipped"
}


def parse_datetime(value):
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")


def validate_event(row):

    # 1. Validate required fields
    required_fields = [
        "event_id",
        "event_type",
        "order_id",
        "customer_id",
        "event_time",
        "ingestion_time",
        "source_offset",
        "is_late"
    ]

    for field in required_fields:
        if not (row.get(field) or "").strip():
            raise ValueError(f"Missing {field}")

    # 2. Validate event type
    if row["event_type"] not in VALID_EVENT_TYPES:
        raise ValueError("Invalid event_type")

    # 3. Validate timestamps
    try:
        event_time = parse_datetime(row["event_time"])
        ingestion_time = parse_datetime(row["ingestion_time"])
    except (ValueError, TypeError):
        raise ValueError("Invalid timestamp format")

    # 4. Validate amount
    try:
        amount = (
            Decimal(row["amount"])
            if (row.get("amount") or "").strip()
            else None
        )

        if amount is not None:
            if not amount.is_finite():
                raise ValueError("Amount must be a finite number")

            if amount < 0:
                raise ValueError("Negative amount")

    except InvalidOperation:
        raise ValueError("Invalid amount")

    # 5. Validate source offset
    try:
        source_offset = int(row["source_offset"])
    except (ValueError, TypeError):
        raise ValueError("Invalid source_offset")

    if source_offset <= 0:
        raise ValueError("source_offset must be positive")

    # 6. Validate late flag
    try:
        is_late = int(row["is_late"])
    except (ValueError, TypeError):
        raise ValueError("Invalid is_late value")

    if is_late not in (0, 1):
        raise ValueError("is_late must be 0 or 1")

    # 7. Return validated event
    return {
        "event_id": row["event_id"].strip(),
        "event_type": row["event_type"],
        "order_id": row["order_id"].strip(),
        "customer_id": row["customer_id"].strip(),
        "amount": amount,
        "event_time": event_time,
        "ingestion_time": ingestion_time,
        "source_offset": source_offset,
        "batch_id": row.get("batch_id"),
        "is_late": is_late
    }



def load_events():

    connection = get_connection()
    cursor = connection.cursor()

    total_read = 0
    inserted = 0
    duplicates = 0
    rejected = 0

    print("Starting event loading...")

    try:
        with open(CSV_FILE, "r", newline="", encoding="utf-8") as file:

            reader = csv.DictReader(file)

            for row in reader:

                total_read += 1

                try:
                    # Validate and normalize the event
                    event = validate_event(row)

                    # Check duplicate event ID
                    cursor.execute(
                        """
                        SELECT 1
                        FROM dbo.events
                        WHERE event_id = ?
                        """,
                        event["event_id"]
                    )

                    if cursor.fetchone():
                        duplicates += 1
                        continue

                    # Insert valid event
                    cursor.execute(
                        """
                        INSERT INTO dbo.events (
                            event_id,
                            event_type,
                            order_id,
                            customer_id,
                            amount,
                            event_time,
                            ingestion_time,
                            source_offset,
                            batch_id,
                            is_late
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        event["event_id"],
                        event["event_type"],
                        event["order_id"],
                        event["customer_id"],
                        event["amount"],
                        event["event_time"],
                        event["ingestion_time"],
                        event["source_offset"],
                        event["batch_id"],
                        event["is_late"]
                    )

                    inserted += 1

                except (ValueError, KeyError, TypeError) as error:

                    # Store invalid event in quarantine
                    cursor.execute(
                        """
                        INSERT INTO dbo.event_quarantine (
                            event_id,
                            raw_event,
                            error_reason
                        )
                        VALUES (?, ?, ?)
                        """,
                        row.get("event_id"),
                        json.dumps(row),
                        str(error)
                    )

                    rejected += 1

        connection.commit()

    except Exception as error:
        connection.rollback()
        print("Loading failed:", error)
        raise

    finally:
        cursor.close()
        connection.close()

    # Prepare ingestion metrics
    metrics = {
        "records_read": total_read,
        "records_inserted": inserted,
        "duplicates_skipped": duplicates,
        "records_rejected": rejected
    }

    print("\n========== LOAD SUMMARY ==========")
    print("Records read      :", total_read)
    print("Records inserted  :", inserted)
    print("Duplicates skipped:", duplicates)
    print("Records rejected  :", rejected)
    print("Batch committed successfully!")

    return metrics

if __name__ == "__main__":
    load_events()
