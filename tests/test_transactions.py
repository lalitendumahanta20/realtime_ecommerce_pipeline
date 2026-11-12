
from datetime import datetime
from types import SimpleNamespace
from decimal import Decimal

import pytest

from process_batches import (
    run_batch_transaction,
    process_one_batch
)


class FakeConnection:

    def __init__(self):
        self.committed = False
        self.rolled_back = False

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


def test_successful_transaction_commits():

    conn = FakeConnection()

    result = run_batch_transaction(
        conn,
        lambda: "SUCCESS"
    )

    assert result == "SUCCESS"
    assert conn.committed is True
    assert conn.rolled_back is False


def test_failed_transaction_rolls_back():

    conn = FakeConnection()

    def failing_operation():
        raise RuntimeError("Simulated batch failure")

    with pytest.raises(
        RuntimeError,
        match="Simulated batch failure"
    ):
        run_batch_transaction(conn, failing_operation)

    assert conn.committed is False
    assert conn.rolled_back is True


def test_failed_transaction_does_not_commit():

    conn = FakeConnection()

    def failing_operation():
        raise ValueError("Database operation failed")

    with pytest.raises(ValueError):
        run_batch_transaction(conn, failing_operation)

    assert conn.committed is False


def test_batch_failure_rolls_back_and_does_not_advance_checkpoint(
    monkeypatch
):

    conn = FakeConnection()

    # Create a sample event.
    rows = [
        SimpleNamespace(
            event_id="TEST_104",
            event_type="order_created",
            amount=Decimal("100.00"),
            event_time=datetime(2026, 11, 12, 10, 15, 0),
            source_offset=104
        )
    ]

    # Mock database-related operations before the failure.
    monkeypatch.setattr(
        "process_batches.get_watermark_details",
        lambda cursor: None
    )

    monkeypatch.setattr(
        "process_batches.save_watermark",
        lambda *args: None
    )

    monkeypatch.setattr(
        "process_batches.update_event_lateness",
        lambda *args: 0
    )

    monkeypatch.setattr(
        "process_batches.aggregate_events",
        lambda rows: {}
    )

    # Simulate a database failure while saving aggregates.
    def failing_save_aggregates(cursor, aggregates):
        raise RuntimeError("Simulated aggregate database failure")

    monkeypatch.setattr(
        "process_batches.save_aggregates",
        failing_save_aggregates
    )

    # Track whether checkpoint advancement was attempted.
    checkpoint_calls = []

    monkeypatch.setattr(
        "process_batches.advance_checkpoint",
        lambda *args: checkpoint_calls.append(args)
    )

    # Run the actual batch operation inside its transaction wrapper.
    with pytest.raises(
        RuntimeError,
        match="Simulated aggregate database failure"
    ):
        run_batch_transaction(
            conn,
            lambda: process_one_batch(object(), rows)
        )

    # Verify failure recovery.
    assert conn.committed is False
    assert conn.rolled_back is True

    # Checkpoint must not advance after the failed aggregate operation.
    assert checkpoint_calls == []


def test_failed_batch_can_be_retried_successfully():

    conn = FakeConnection()
    attempts = {"count": 0}

    def retry_operation():
        attempts["count"] += 1

        if attempts["count"] == 1:
            raise RuntimeError("Temporary database failure")

        return "SUCCESS"

    # First attempt should fail and roll back.
    with pytest.raises(RuntimeError):
        run_batch_transaction(conn, retry_operation)

    assert conn.rolled_back is True
    assert conn.committed is False

    # Second attempt should succeed.
    result = run_batch_transaction(conn, retry_operation)

    assert result == "SUCCESS"
    assert attempts["count"] == 2
    assert conn.committed is True

