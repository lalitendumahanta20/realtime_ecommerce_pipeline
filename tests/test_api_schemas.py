import pytest
from pydantic import ValidationError
from api.schemas import EventBatch


def valid_event(**overrides):
    event = {
        'event_id': 'evt-1', 'event_type': 'order_created',
        'order_id': 'ord-1', 'customer_id': 'cust-1', 'amount': '99.50',
        'event_time': '2026-11-16T10:00:00', 'source_offset': 1
    }
    event.update(overrides)
    return event


def test_valid_batch_parses():
    batch = EventBatch(events=[valid_event()])
    assert len(batch.events) == 1
    assert batch.events[0].event_id == 'evt-1'


def test_negative_amount_is_rejected():
    with pytest.raises(ValidationError):
        EventBatch(events=[valid_event(amount='-1')])


def test_invalid_event_type_is_rejected():
    with pytest.raises(ValidationError):
        EventBatch(events=[valid_event(event_type='user_login')])


def test_zero_offset_is_rejected():
    with pytest.raises(ValidationError):
        EventBatch(events=[valid_event(source_offset=0)])


def test_blank_event_id_is_rejected():
    with pytest.raises(ValidationError):
        EventBatch(events=[valid_event(event_id='  ')])


def test_unknown_fields_are_rejected():
    with pytest.raises(ValidationError):
        EventBatch(events=[valid_event(unexpected='not-allowed')])


def test_api_health_endpoint():
    from fastapi.testclient import TestClient
    from api.main import app
    response = TestClient(app).get('/health')
    assert response.status_code == 200
    assert response.json()['status'] == 'ok'


def test_api_rejects_invalid_batch_before_database_call():
    from fastapi.testclient import TestClient
    from api.main import app
    response = TestClient(app).post('/api/v1/events', json={'events': [valid_event(amount=-5)]})
    assert response.status_code == 422
