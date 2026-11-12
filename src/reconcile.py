
from db_connection import get_connection


RECONCILIATION_QUERY = """
WITH source_summary AS (
    SELECT
        event_type,
        COUNT(*) AS event_count,
        SUM(COALESCE(amount, 0)) AS total_amount
    FROM dbo.events
    GROUP BY event_type
),
aggregate_summary AS (
    SELECT
        event_type,
        SUM(event_count) AS event_count,
        SUM(total_amount) AS total_amount
    FROM dbo.minute_aggregates
    GROUP BY event_type
)
SELECT
    COALESCE(s.event_type, a.event_type) AS event_type,
    COALESCE(s.event_count, 0) AS source_count,
    COALESCE(a.event_count, 0) AS aggregate_count,
    COALESCE(s.total_amount, 0) AS source_amount,
    COALESCE(a.total_amount, 0) AS aggregate_amount,
    COALESCE(s.event_count, 0)
        - COALESCE(a.event_count, 0) AS count_difference,
    COALESCE(s.total_amount, 0)
        - COALESCE(a.total_amount, 0) AS amount_difference
FROM source_summary s
FULL OUTER JOIN aggregate_summary a
    ON s.event_type = a.event_type
ORDER BY event_type;
"""


def fetch_reconciliation_results(cursor):
    cursor.execute(RECONCILIATION_QUERY)
    return cursor.fetchall()


def reconcile_data():
    conn = get_connection()
    cursor = conn.cursor()

    try:
        results = fetch_reconciliation_results(cursor)

        print("\n========== DATA RECONCILIATION ==========")

        all_matched = True

        for row in results:
            event_type = row[0]
            count_difference = row[5]
            amount_difference = row[6]

            status = (
                "MATCHED"
                if count_difference == 0 and amount_difference == 0
                else "MISMATCH"
            )

            if status == "MISMATCH":
                all_matched = False

            print(
                f"{event_type}: {status} | "
                f"Count difference: {count_difference} | "
                f"Amount difference: {amount_difference}"
            )

        if all_matched:
            print("\nReconciliation successful!")
        else:
            print("\nReconciliation failed! Check the mismatched records.")

        return all_matched

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    reconcile_data()
