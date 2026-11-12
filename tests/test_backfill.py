
from process_batches import rebuild_minute_aggregates


class FakeCursor:

    def __init__(self):
        self.queries = []
        self.rowcount = 5

    def execute(self, query, *params):
        self.queries.append(query)


def test_backfill_deletes_before_inserting():

    cursor = FakeCursor()

    rebuild_minute_aggregates(cursor)

    assert len(cursor.queries) == 2

    assert "DELETE FROM dbo.minute_aggregates" in (
        cursor.queries[0]
    )

    assert "INSERT INTO dbo.minute_aggregates" in (
        cursor.queries[1]
    )


def test_backfill_rebuilds_from_events_table():

    cursor = FakeCursor()

    rebuild_minute_aggregates(cursor)

    insert_query = cursor.queries[1].upper()

    assert "FROM DBO.EVENTS" in insert_query
    assert "GROUP BY" in insert_query
    assert "EVENT_TYPE" in insert_query
    assert "EVENT_TIME" in insert_query
    assert "SUM(COALESCE(AMOUNT, 0))" in insert_query
