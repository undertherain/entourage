"""Transport-neutral ingress: routing, dedup, await vertical, late results.

The router terminates any transport's delivery as one idempotent mailbox
append; destinations resolve at ingress (explicit route → live await
conversation → transport hint → default inbox). The await vertical is the
acceptance case: a parked graph resumes when a correlated result arrives,
with no transport knowledge anywhere in the graph.
"""

import time

import pytest

from entourage.flow import Sequence, WaitForMailbox
from entourage.ingress import (
    InboundEvent,
    IngressRouter,
    correlation_conversation,
)
from entourage.mailbox import InMemoryMailbox
from entourage.runtime import (
    InMemoryGraphStore,
    InMemoryReadyQueue,
    QueueRuntime,
)


@pytest.fixture
def mailbox():
    return InMemoryMailbox()


def make_runtime(mailbox, registry):
    return QueueRuntime(
        node_registry=registry,
        store=InMemoryGraphStore(),
        queue=InMemoryReadyQueue(),
        mailbox=mailbox,
    )


def drain(rt, rounds=20):
    for _ in range(rounds):
        rt.run_once(poll_wait=0.01)


def result_event(correlation="op-1", event_id="r1", payload=None):
    return InboundEvent(
        event_id=event_id,
        kind="result",
        source="claude-remote",
        payload=payload or {"answer": 42},
        correlation_id=correlation,
    )


def test_await_vertical(mailbox):
    """A parked graph resumes when the correlated result arrives."""
    sink = []

    def consume(state):
        sink.append(state["events"])
        return state

    rt = make_runtime(mailbox, {"consume": consume})
    conversation = correlation_conversation("op-1")
    sid = rt.start_session(
        Sequence(WaitForMailbox(conversation=conversation), "consume")
    )
    drain(rt)
    assert rt.waiting_conversations() == {conversation}

    router = IngressRouter(
        mailbox,
        waiting_conversations=rt.waiting_conversations,
        wake=rt.wake_due_waits,
    )
    delivered_to = router.accept(result_event())
    assert delivered_to == conversation
    drain(rt)

    assert rt.store.get_session(sid)["status"] == "completed"
    (events,) = sink
    assert events[0]["payload"] == {"answer": 42}
    assert events[0]["correlation_id"] == "op-1"


def test_duplicate_delivery_is_absorbed(mailbox):
    router = IngressRouter(mailbox, default_conversation="inbox")
    router.accept(result_event())
    router.accept(result_event())  # transport redelivery
    assert mailbox.claimable_count("inbox") == 1


def test_late_result_falls_to_inbox(mailbox):
    """No live waiter for the correlation → resident inbox, as ambient."""
    router = IngressRouter(
        mailbox,
        default_conversation="inbox",
        waiting_conversations=lambda: set(),
    )
    assert router.accept(result_event()) == "inbox"


def test_explicit_route_wins(mailbox):
    router = IngressRouter(
        mailbox,
        default_conversation="inbox",
        waiting_conversations=lambda: {correlation_conversation("op-1")},
    )
    router.register("op-1", "agent:concierge")
    assert router.accept(result_event()) == "agent:concierge"


def test_expired_route_falls_through(mailbox):
    router = IngressRouter(mailbox, default_conversation="inbox")
    router.register("op-1", "agent:concierge", expires_at=time.time() - 1)
    assert router.accept(result_event()) == "inbox"


def test_transport_hint_before_default(mailbox):
    router = IngressRouter(mailbox, default_conversation="inbox")
    event = InboundEvent(
        event_id="a1",
        kind="ambient",
        source="grafana",
        conversation_id="agent:kip",
    )
    assert router.accept(event) == "agent:kip"


def test_no_destination_raises(mailbox):
    router = IngressRouter(mailbox)
    with pytest.raises(ValueError):
        router.accept(result_event())
    assert mailbox.claimable_count("inbox") == 0
