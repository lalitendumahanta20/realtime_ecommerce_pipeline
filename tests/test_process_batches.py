
from datetime import datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

from process_batches import (
    calculate_watermark,
    is_late_event,
    aggregate_events,
    fetch_next_batch
)


class FakeCursor:
    """
    Simulates the database cursor for testing batch selection.
    No real SQL Server changes are made.
    """

    def __init__(self, events):
        self.events = events
        self.query = ""
        self.params = ()

    def execute(self, query, *params):
        self.query = query
        self.params = params

    def fetchall(self):
        last_offset = self.params[0]

        return [
            event
            for event in self.events
            if event.source_offset > last_offset
        ][:10]


def make_event(event_id, event_type, amount, event_time, offset):
    return SimpleNamespace(
        event_id=event_id,
        event_type=event_type,
        amount=amount,
        event_time=event_time,
        source_offset=offset
    )


def test_watermark_calculation():
    max_event_time = datetime(2026, 11, 12, 10, 15, 0)

    max_time, watermark = calculate_watermark(
        None,
        max_event_time,
        5
    )

    assert max_time == max_event_time
    assert watermark == datetime(2026, 11, 12, 10, 10, 0)


def test_watermark_never_moves_backwards():
    old_max = datetime(2026, 11, 12, 10, 15, 0)
    new_batch_max = datetime(2026, 11, 12, 10, 5, 0)

    max_time, watermark = calculate_watermark(
        old_max,
        new_batch_max,
        5
    )

    assert max_time == old_max
    assert watermark == datetime(2026, 11, 12, 10, 10, 0)


def test_late_event_detection():
    watermark = datetime(2026, 11, 12, 10, 10, 0)
    event_time = datetime(2026, 11, 12, 9, 15, 0)

    assert is_late_event(event_time, watermark) is True


def test_on_time_event_detection():
    watermark = datetime(2026, 11, 12, 10, 10, 0)
    event_time = datetime(2026, 11, 12, 10, 15, 0)

    assert is_late_event(event_time, watermark) is False


def test_event_exactly_at_watermark_is_not_late():
    watermark = datetime(2026, 11, 12, 10, 10, 0)

    assert is_late_event(watermark, watermark) is False


def test_amount_and_event_aggregation():
    event_time = datetime(2026, 11, 12, 10, 15, 30)

    events = [
        make_event(
            "EVT001", "order_created",
            Decimal("100.00"), event_time, 1
        ),
        make_event(
            "EVT002", "order_created",
            Decimal("200.00"), event_time, 2
        ),
        make_event(
            "EVT003", "payment_completed",
            Decimal("300.00"), event_time, 3
        )
    ]

    result = aggregate_events(events)

    order_key = (
        datetime(2026, 11, 12, 10, 15, 0),
        "order_created"
    )

    payment_key = (
        datetime(2026, 11, 12, 10, 15, 0),
        "payment_completed"
    )

    assert result[order_key]["count"] == 2
    assert result[order_key]["amount"] == Decimal("300.00")

    assert result[payment_key]["count"] == 1
    assert result[payment_key]["amount"] == Decimal("300.00")


def test_empty_aggregation():
    result = aggregate_events([])

    assert len(result) == 0


def test_fetch_next_batch_after_checkpoint():
    event_time = datetime(2026, 11, 12, 10, 15, 0)

    events = [
        make_event(
            "EVT101", "order_created",
            Decimal("100"), event_time, 101
        ),
        make_event(
            "EVT102", "order_created",
            Decimal("200"), event_time, 102
        ),
        make_event(
            "EVT103", "order_created",
            Decimal("300"), event_time, 103
        ),
        make_event(
            "EVT104", "order_created",
            Decimal("400"), event_time, 104
        )
    ]

    cursor = FakeCursor(events)

    result = fetch_next_batch(cursor, 103)

    assert len(result) == 1
    assert result[0].source_offset == 104
    assert "source_offset > ?" in cursor.query
    assert cursor.params == (103,)


def test_restart_does_not_fetch_committed_events():
    event_time = datetime(2026, 11, 12, 10, 15, 0)

    events = [
        make_event(
            "EVT101", "order_created",
            Decimal("100"), event_time, 101
        ),
        make_event(
            "EVT102", "order_created",
            Decimal("200"), event_time, 102
        ),
        make_event(
            "EVT103", "order_created",
            Decimal("300"), event_time, 103
        )
    ]

    # Simulate restarting after checkpoint 103.
    cursor = FakeCursor(events)

    result = fetch_next_batch(cursor, 103)

    assert result == []


def test_empty_batch_after_checkpoint():
    cursor = FakeCursor([])

    result = fetch_next_batch(cursor, 103)

    assert result == []


def test_new_events_are_fetched_after_checkpoint():
    event_time = datetime(2026, 11, 12, 10, 15, 0)

    events = [
        make_event(
            "EVT104", "order_created",
            Decimal("400"), event_time, 104
        ),
        make_event(
            "EVT105", "payment_completed",
            Decimal("500"), event_time, 105
        )
    ]

    cursor = FakeCursor(events)

    result = fetch_next_batch(cursor, 103)

    assert len(result) == 2
    assert [event.source_offset for event in result] == [104, 105]
