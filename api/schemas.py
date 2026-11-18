from datetime import datetime
from decimal import Decimal
from typing import Literal, Optional
from pydantic import BaseModel, Field, ConfigDict, field_validator

EventType = Literal['order_created', 'payment_completed', 'order_cancelled', 'order_shipped']

class EventIn(BaseModel):
    model_config = ConfigDict(extra='forbid')
    event_id: str = Field(min_length=1, max_length=100)
    event_type: EventType
    order_id: Optional[str] = Field(default=None, max_length=50)
    customer_id: Optional[str] = Field(default=None, max_length=50)
    amount: Optional[Decimal] = Field(default=None, ge=0, max_digits=12, decimal_places=2)
    event_time: datetime
    ingestion_time: Optional[datetime] = None
    source_offset: int = Field(gt=0)
    batch_id: Optional[str] = Field(default=None, max_length=50)

    @field_validator('event_id')
    @classmethod
    def strip_event_id(cls, value):
        value = value.strip()
        if not value:
            raise ValueError('event_id cannot be blank')
        return value

class EventBatch(BaseModel):
    model_config = ConfigDict(extra='forbid')
    events: list[EventIn] = Field(min_length=1, max_length=1000)

class IngestResponse(BaseModel):
    status: str
    received: int
    inserted: int
    duplicates: int
