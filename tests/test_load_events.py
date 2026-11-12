
import pytest
from datetime import datetime
from decimal import Decimal

from load_events import validate_event


def valid_event():
    return {
        "event_id": "EVT001",
        "event_type": "order_created",
        "order_id": "ORD001",
        "customer_id": "CUS001",
        "amount": "150.50",
        "event_time": "2026-11-12 10:00:00",
        "ingestion_time": "2026-11-12 10:01:00",
        "source_offset": "1",
        "batch_id": "batch_001",
        "is_late": "0"
    }


def test_valid_event():
    event = validate_event(valid_event())

    assert event["event_id"] == "EVT001"
    assert event["amount"] == Decimal("150.50")
    assert isinstance(event["event_time"], datetime)
    assert event["source_offset"] == 1
    assert event["is_late"] == 0


@pytest.mark.parametrize("field", [
    "event_id",
    "event_type",
    "order_id",
    "customer_id",
    "event_time",
    "ingestion_time",
    "source_offset",
    "is_late"
])
def test_missing_required_fields(field):
    row = valid_event()
    row[field] = ""

    with pytest.raises(ValueError):
        validate_event(row)


def test_invalid_event_type():
    row = valid_event()
    row["event_type"] = "invalid_event"

    with pytest.raises(ValueError, match="Invalid event_type"):
        validate_event(row)


def test_invalid_timestamp():
    row = valid_event()
    row["event_time"] = "wrong-date"

    with pytest.raises(ValueError, match="Invalid timestamp format"):
        validate_event(row)


def test_invalid_amount():
    row = valid_event()
    row["amount"] = "abc"

    with pytest.raises(ValueError, match="Invalid amount"):
        validate_event(row)


def test_negative_amount():
    row = valid_event()
    row["amount"] = "-100"

    with pytest.raises(ValueError, match="Negative amount"):
        validate_event(row)


@pytest.mark.parametrize("amount", ["NaN", "Infinity", "-Infinity"])
def test_non_finite_amount(amount):
    row = valid_event()
    row["amount"] = amount

    with pytest.raises(ValueError, match="finite number"):
        validate_event(row)


@pytest.mark.parametrize("offset", ["abc", "0", "-1"])
def test_invalid_source_offset(offset):
    row = valid_event()
    row["source_offset"] = offset

    with pytest.raises(ValueError):
        validate_event(row)


def test_invalid_late_flag():
    row = valid_event()
    row["is_late"] = "2"

    with pytest.raises(ValueError, match="is_late must be 0 or 1"):
        validate_event(row)


def test_empty_amount_is_allowed():
    row = valid_event()
    row["amount"] = ""

    event = validate_event(row)

    assert event["amount"] is None
