import uuid

from producer.src.producer import (
    create_order_event,
    produce_events,
)


def test_order_id_uses_separate_uuid(
    monkeypatch,
):
    generated = iter(
        [
            uuid.UUID("11111111-1111-1111-1111-111111111111"),
            uuid.UUID("22222222-2222-2222-2222-123456789abc"),
        ]
    )

    monkeypatch.setattr(
        "producer.src.producer.uuid.uuid4",
        lambda: next(generated),
    )

    event = create_order_event()

    assert event["event_id"] == "11111111-1111-1111-1111-111111111111"

    assert event["order_id"] == "22222222-2222-2222-2222-123456789abc"


class FakeProducer:
    def __init__(self):
        self.sent = []

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))


def test_produce_events_respects_count():
    producer = FakeProducer()

    produced = produce_events(
        producer,
        count=3,
        interval=0,
        quiet=True,
    )

    assert produced == 3
    assert len(producer.sent) == 3
