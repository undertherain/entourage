import threading
import time

import pytest

from entourage.mailbox import InMemoryMailbox


def test_append_is_idempotent_and_claims_in_order():
    mailbox = InMemoryMailbox()
    mailbox.append("chat", {"event_id": "one", "kind": "user", "content": "first"})
    mailbox.append("chat", {"event_id": "one", "kind": "user", "content": "duplicate"})
    mailbox.append("chat", {"event_id": "two", "kind": "ambient", "content": "second"})

    events = mailbox.claim("chat", "worker", limit=10)

    assert [event["event_id"] for event in events] == ["one", "two"]
    assert events[0]["content"] == "first"


def test_acknowledged_events_are_not_redelivered():
    mailbox = InMemoryMailbox()
    event_id = mailbox.append("chat", {"kind": "user", "content": "hello"})
    mailbox.claim("chat", "worker")
    mailbox.acknowledge("chat", "worker", [event_id])

    assert mailbox.claim("chat", "worker") == []


def test_acknowledged_events_can_be_pruned_in_bounded_batches():
    mailbox = InMemoryMailbox()
    ids = [mailbox.append("chat", {"event_id": str(index)}) for index in range(3)]
    mailbox.claim("chat", "worker")
    mailbox.acknowledge("chat", "worker", ids)

    assert mailbox.purge_acknowledged("chat", limit=2) == 2
    assert mailbox.purge_acknowledged("chat", limit=2) == 1
    assert mailbox.purge_acknowledged("chat", limit=2) == 0

    # Payload GC retains a lightweight idempotency tombstone.
    assert mailbox.append("chat", {"event_id": "0"}) == "0"
    assert mailbox.claim("chat", "worker") == []
    assert mailbox.purge_deduplication_keys(time.time() + 1, limit=2) == 2
    assert mailbox.append("chat", {"event_id": "0"}) == "0"
    assert mailbox.claim("chat", "worker")[0]["event_id"] == "0"


def test_release_and_expired_lease_redeliver():
    mailbox = InMemoryMailbox()
    first = mailbox.append("chat", {"event_id": "first"})
    second = mailbox.append("chat", {"event_id": "second"})
    mailbox.claim("chat", "crashed", limit=1, lease_seconds=0)
    leased = mailbox.claim("chat", "worker", limit=1)
    assert leased[0]["event_id"] == first
    mailbox.acknowledge("chat", "worker", [first])

    mailbox.claim("chat", "worker", limit=1)
    mailbox.release("chat", "worker", [second])
    assert mailbox.claim("chat", "next", limit=1)[0]["event_id"] == second


def test_wrong_consumer_cannot_acknowledge():
    mailbox = InMemoryMailbox()
    event_id = mailbox.append("chat", {"content": "hello"})
    mailbox.claim("chat", "owner")

    with pytest.raises(ValueError, match="not leased"):
        mailbox.acknowledge("chat", "stranger", [event_id])


def test_wait_wakes_when_an_event_arrives():
    mailbox = InMemoryMailbox()
    observed = []

    def wait():
        observed.append(mailbox.wait_for_events("chat", timeout=1))

    thread = threading.Thread(target=wait)
    thread.start()
    time.sleep(0.01)
    mailbox.append("chat", {"content": "wake"})
    thread.join()

    assert observed == [True]


def test_wait_wakes_when_a_lease_expires():
    mailbox = InMemoryMailbox()
    mailbox.append("chat", {"content": "recover"})
    mailbox.claim("chat", "crashed", lease_seconds=0.01)

    assert mailbox.wait_for_events("chat", timeout=0.5) is True
    assert mailbox.claim("chat", "recovery")[0]["content"] == "recover"


def test_claim_any_selects_oldest_conversation_without_mixing():
    mailbox = InMemoryMailbox()
    mailbox.append("older", {"event_id": "one", "created_at": 1})
    mailbox.append("newer", {"event_id": "two", "created_at": 2})
    mailbox.append("older", {"event_id": "three", "created_at": 3})

    events = mailbox.claim_any("worker")

    assert [event["event_id"] for event in events] == ["one", "three"]
    assert {event["conversation_id"] for event in events} == {"older"}


def test_wait_for_any_conversation():
    mailbox = InMemoryMailbox()
    mailbox.append("chat-b", {"content": "ready"})

    assert mailbox.wait_for_events(timeout=0) is True
