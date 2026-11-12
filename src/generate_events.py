
import csv
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

# Reproducible sample data
random.seed(42)

# Project data folder
PROJECT_DIR = Path(__file__).resolve().parent.parent
OUTPUT_FILE = PROJECT_DIR / "data" / "sample_events.csv"

EVENT_TYPES = [
    "order_created",
    "payment_completed",
    "order_cancelled",
    "order_shipped"
]

def generate_events(number_of_events=1000, starting_offset=104):
    events = []

    now = datetime.utcnow()

    for offset in range(starting_offset, starting_offset + number_of_events):

        event_type = random.choice(EVENT_TYPES)

        # Generate some events with older event timestamps
        if random.random() < 0.10:
            event_time = now - timedelta(
                minutes=random.randint(10, 20)
            )
        else:
            event_time = now - timedelta(
                minutes=random.randint(0, 9)
            )

        # Only financial events have an amount
        if event_type in ["order_created", "payment_completed"]:
            amount = round(random.uniform(100, 5000), 2)
        else:
            amount = None

        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "order_id": f"ORD{random.randint(1000, 1050)}",
            "customer_id": f"CUST{random.randint(1, 30):03d}",
            "amount": amount,
            "event_time": event_time.strftime("%Y-%m-%d %H:%M:%S"),
            "ingestion_time": now.strftime("%Y-%m-%d %H:%M:%S"),
            "source_offset": offset,
            "batch_id": "performance_test_001",
            "is_late": 0
        }

        events.append(event)

    return events


def save_events(events):
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=events[0].keys()
        )
        writer.writeheader()
        writer.writerows(events)

    print(f"Successfully generated {len(events)} events")
    print(f"First source offset: {events[0]['source_offset']}")
    print(f"Last source offset: {events[-1]['source_offset']}")
    print(f"CSV file saved at: {OUTPUT_FILE}")


if __name__ == "__main__":
    sample_events = generate_events(1000, starting_offset=104)
    save_events(sample_events)
