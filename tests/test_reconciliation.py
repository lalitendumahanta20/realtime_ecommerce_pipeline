
from reconcile import fetch_reconciliation_results


class FakeCursor:
    def __init__(self, results):
        self.results = results
        self.executed_query = None

    def execute(self, query):
        self.executed_query = query

    def fetchall(self):
        return self.results


def test_reconciliation_returns_matching_results():
    expected = [
        ("order_created", 30, 30, 68431.89, 68431.89, 0, 0.00),
        ("payment_completed", 24, 24, 71099.03, 71099.03, 0, 0.00),
    ]

    cursor = FakeCursor(expected)

    results = fetch_reconciliation_results(cursor)

    assert results == expected
    assert "FULL OUTER JOIN" in cursor.executed_query


def test_reconciliation_detects_count_mismatch():
    expected = [
        ("order_created", 30, 29, 68431.89, 68431.89, 1, 0.00)
    ]

    cursor = FakeCursor(expected)

    results = fetch_reconciliation_results(cursor)

    assert results[0][5] == 1


def test_reconciliation_detects_amount_mismatch():
    expected = [
        ("payment_completed", 24, 24, 71099.03, 70000.00, 0, 1099.03)
    ]

    cursor = FakeCursor(expected)

    results = fetch_reconciliation_results(cursor)

    assert results[0][6] == 1099.03


def test_reconciliation_handles_empty_results():
    cursor = FakeCursor([])

    results = fetch_reconciliation_results(cursor)

    assert results == []
