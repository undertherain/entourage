import time


def test_mailbox_backend_idempotent_claim_ack_and_gc(mailbox_backend):
    mailbox = mailbox_backend
    first = mailbox.append("chat", {"event_id": "one", "content": "first"})
    mailbox.append("chat", {"event_id": "one", "content": "duplicate"})
    second = mailbox.append("chat", {"event_id": "two", "content": "second"})

    events = mailbox.claim("chat", "worker")
    assert [event["content"] for event in events] == ["first", "second"]
    mailbox.acknowledge("chat", "worker", [first, second])
    assert mailbox.claim("chat", "worker") == []
    assert mailbox.purge_acknowledged("chat", limit=1) == 1
    assert mailbox.purge_acknowledged("chat", limit=10) == 1

    # Payload collection keeps deduplication effective until its own horizon.
    mailbox.append("chat", {"event_id": "one", "content": "redelivery"})
    assert mailbox.claim("chat", "worker") == []


def test_mailbox_backend_claim_any_does_not_mix_conversations(mailbox_backend):
    mailbox = mailbox_backend
    mailbox.append("first", {"event_id": "one"})
    mailbox.append("second", {"event_id": "two"})
    mailbox.append("first", {"event_id": "three"})

    events = mailbox.claim_any("worker")

    assert [event["event_id"] for event in events] == ["one", "three"]
    assert {event["conversation_id"] for event in events} == {"first"}


def test_mailbox_backend_recovers_expired_lease(mailbox_backend):
    mailbox = mailbox_backend
    mailbox.append("chat", {"event_id": "recover"})
    mailbox.claim("chat", "crashed", lease_seconds=0)

    assert mailbox.wait_for_events("chat", timeout=0.5) is True
    assert mailbox.claim("chat", "recovery")[0]["event_id"] == "recover"


def test_mailbox_backend_releases_and_expires_tombstones(mailbox_backend):
    mailbox = mailbox_backend
    event_id = mailbox.append("chat", {"event_id": "event"})
    mailbox.claim("chat", "worker")
    mailbox.release("chat", "worker", [event_id])
    mailbox.claim("chat", "worker")
    mailbox.acknowledge("chat", "worker", [event_id])
    mailbox.purge_acknowledged("chat")

    assert mailbox.purge_deduplication_keys(time.time() + 1) == 1
    mailbox.append("chat", {"event_id": "event"})
    assert mailbox.claim("chat", "next")[0]["event_id"] == "event"
