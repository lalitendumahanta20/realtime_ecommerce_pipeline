
"""Automated data-quality checks for the e-commerce pipeline."""

from datetime import datetime, timezone
from decimal import Decimal

from src.db_connection import get_connection


SAMPLE_LIMIT = 20


def serialize_value(value):
    """Convert SQL values into JSON-friendly values."""
    if isinstance(value, (datetime,)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return value


def fetch_records(cursor):
    """Convert the current SQL result into dictionaries."""
    columns = [column[0] for column in cursor.description]
    records = []

    for row in cursor.fetchall():
        records.append({
            column: serialize_value(value)
            for column, value in zip(columns, row)
        })

    return records


def build_check(name, issue_count, samples):
    return {
        "check": name,
        "status": "PASS" if issue_count == 0 else "FAIL",
        "issue_count": issue_count,
        "samples": samples
    }


def check_missing_events(cursor):
    """Find source events absent from the processed-events ledger."""

    cursor.execute("""
        SELECT COUNT(*)
        FROM dbo.events e
        LEFT JOIN dbo.processed_events p
            ON e.event_id = p.event_id
        WHERE p.event_id IS NULL;
    """)

    issue_count = cursor.fetchone()[0]

    cursor.execute("""
        SELECT TOP (20)
            e.event_id,
            e.source_offset,
            e.event_type
        FROM dbo.events e
        LEFT JOIN dbo.processed_events p
            ON e.event_id = p.event_id
        WHERE p.event_id IS NULL
        ORDER BY e.source_offset;
    """)

    return build_check(
        "missing_events",
        issue_count,
        fetch_records(cursor)
    )


def check_duplicate_events(cursor):
    """Detect duplicate event IDs and source offsets."""

    cursor.execute("""
        SELECT COUNT(*)
        FROM (
            SELECT event_id
            FROM dbo.events
            GROUP BY event_id
            HAVING COUNT(*) > 1

            UNION ALL

            SELECT CAST(source_offset AS VARCHAR(30))
            FROM dbo.events
            GROUP BY source_offset
            HAVING COUNT(*) > 1
        ) AS duplicates;
    """)

    issue_count = cursor.fetchone()[0]

    cursor.execute("""
        SELECT TOP (20) *
        FROM (
            SELECT
                'event_id' AS duplicate_type,
                event_id AS duplicate_value,
                COUNT(*) AS occurrences
            FROM dbo.events
            GROUP BY event_id
            HAVING COUNT(*) > 1

            UNION ALL

            SELECT
                'source_offset',
                CAST(source_offset AS VARCHAR(30)),
                COUNT(*)
            FROM dbo.events
            GROUP BY source_offset
            HAVING COUNT(*) > 1
        ) AS duplicate_records;
    """)

    return build_check(
        "duplicate_events",
        issue_count,
        fetch_records(cursor)
    )



def check_invalid_amounts(cursor):
    """Validate amounts according to event-specific business rules."""

    invalid_condition = """
        (
            event_type IN ('order_created', 'payment_completed')
            AND amount IS NULL
        )
        OR amount < 0
    """

    cursor.execute(f"""
        SELECT COUNT(*)
        FROM dbo.events
        WHERE {invalid_condition};
    """)

    issue_count = cursor.fetchone()[0]

    cursor.execute(f"""
        SELECT TOP (20)
            event_id,
            event_type,
            amount
        FROM dbo.events
        WHERE {invalid_condition}
        ORDER BY source_offset;
    """)

    return build_check(
        "invalid_amounts",
        issue_count,
        fetch_records(cursor)
    )


def check_aggregate_mismatch(cursor):
    """
    Compare source events with minute aggregates.
    Grouping uses the same event-minute logic as the pipeline.
    """

    comparison_sql = """
        WITH source_aggregates AS (
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
        ),
        differences AS (
            SELECT
                COALESCE(s.window_start, a.window_start)
                    AS window_start,
                COALESCE(s.event_type, a.event_type)
                    AS event_type,
                COALESCE(s.event_count, 0)
                    AS source_count,
                COALESCE(a.event_count, 0)
                    AS aggregate_count,
                COALESCE(s.total_amount, 0)
                    AS source_amount,
                COALESCE(a.total_amount, 0)
                    AS aggregate_amount
            FROM source_aggregates s
            FULL OUTER JOIN dbo.minute_aggregates a
                ON s.window_start = a.window_start
                AND s.event_type = a.event_type
        )
    """

    cursor.execute(
        comparison_sql + """
        SELECT COUNT(*)
        FROM differences
        WHERE source_count <> aggregate_count
           OR source_amount <> aggregate_amount;
        """
    )

    issue_count = cursor.fetchone()[0]

    cursor.execute(
        comparison_sql + """
        SELECT TOP (20) *
        FROM differences
        WHERE source_count <> aggregate_count
           OR source_amount <> aggregate_amount
        ORDER BY window_start;
        """
    )

    return build_check(
        "aggregate_mismatch",
        issue_count,
        fetch_records(cursor)
    )


def run_quality_checks():
    """Run all quality checks and return a combined report."""

    conn = get_connection()

    try:
        cursor = conn.cursor()

        checks = [
            check_missing_events(cursor),
            check_duplicate_events(cursor),
            check_invalid_amounts(cursor),
            check_aggregate_mismatch(cursor)
        ]

        total_issues = sum(
            check["issue_count"] for check in checks
        )

        return {
            "overall_status": (
                "PASS" if total_issues == 0 else "FAIL"
            ),
            "checked_at": datetime.now(
                timezone.utc
            ).isoformat(),
            "total_checks": len(checks),
            "total_issues": total_issues,
            "checks": checks
        }

    finally:
        conn.close()
