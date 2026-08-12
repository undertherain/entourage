import time

from entourage.runtime import (
    InMemoryGraphStore,
    InMemoryReadyQueue,
    QueueRuntime,
    TriggerClient,
)


def make_runtime(seen=None):
    store = InMemoryGraphStore()
    queue = InMemoryReadyQueue()
    runtime = QueueRuntime(store=store, queue=queue)

    def handle(state):
        if seen is not None:
            seen.append(state["message"])
        return state

    runtime.register_node("handle", handle)
    runtime.register_pipeline("message", lambda _state: "handle")
    return runtime, store


def test_same_serial_key_waits_for_running_session():
    seen = []
    runtime, store = make_runtime(seen)
    runtime.send_trigger("message", {"message": "first"}, serial_key="chat:1")
    runtime.send_trigger("message", {"message": "second"}, serial_key="chat:1")

    # Both triggers arrive in one batch. The first starts; the second is put
    # back because its serialization key is now owned by a running session.
    runtime.run_once(poll_wait=0)
    assert len(store.get_running_sessions()) == 1

    deadline = time.monotonic() + 2
    while len(seen) < 2 and time.monotonic() < deadline:
        runtime.run_once(poll_wait=0.01)

    assert seen == ["first", "second"]
    assert len(store.get_running_sessions()) <= 1


def test_different_serial_keys_can_start_together():
    runtime, store = make_runtime()
    runtime.send_trigger("message", {"message": "one"}, serial_key="chat:1")
    runtime.send_trigger("message", {"message": "two"}, serial_key="chat:2")

    runtime.run_once(poll_wait=0)

    assert len(store.get_running_sessions()) == 2


def test_store_claims_and_releases_serial_key(store):
    first = store.create_session("message", {"message": "one"}, serial_key="chat:1")
    assert first is not None
    assert store.create_session(
        "message", {"message": "two"}, serial_key="chat:1"
    ) is None

    store.complete_session(first)
    second = store.create_session("message", {"message": "two"}, serial_key="chat:1")
    assert second is not None


def test_trigger_client_includes_serial_key():
    queue = InMemoryReadyQueue()
    TriggerClient(queue).send_trigger("message", {"text": "hi"}, "chat:1")
    (message,) = queue.receive(wait_seconds=0)
    assert message.payload["trigger"] == "message"
    assert message.payload["serial_key"] == "chat:1"
