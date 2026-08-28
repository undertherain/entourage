"""WaitForMailbox parking: drain, park, wake, timeout, recovery.

The wait is an execution row in status ``waiting`` with a durable wake
condition; waking flips it back to pending and enqueues it. Every path is
idempotent, so these tests drive the runtime with run_once and explicit
wake ticks. Parametrized over all GraphStore backends via the shared
``store`` fixture.
"""

import time

import pytest

from entourage.flow import Sequence, Parallel, WaitForMailbox
from entourage.mailbox import InMemoryMailbox
from entourage.runtime import InMemoryReadyQueue, QueueRuntime


@pytest.fixture
def mailbox():
    return InMemoryMailbox()


@pytest.fixture
def make_rt(store, mailbox):
    def factory(registry=None):
        return QueueRuntime(
            node_registry=registry or {},
            store=store,
            queue=InMemoryReadyQueue(),
            mailbox=mailbox,
        )

    return factory


def drain(rt, rounds=20):
    """Process queue messages until an empty poll."""
    for _ in range(rounds):
        rt.run_once(poll_wait=0.01)


def seen_events(store, session_id, node="consume"):
    executions = store.get_session_executions(session_id)
    consume = next(ex for ex in executions if ex["node_name"] == node)
    return (consume["input_state"] or {}).get("events")


def make_consume(sink):
    def consume(state):
        sink.append(state.get("events"))
        return state

    return consume


def waiting_execution(store, session_id):
    waiting = [
        ex
        for ex in store.get_session_executions(session_id, status="waiting")
    ]
    return waiting[0] if waiting else None


def test_wait_drains_preexisting_events(make_rt, mailbox, store):
    sink = []
    rt = make_rt({"consume": make_consume(sink)})
    mailbox.append("conv", {"event_id": "e1", "kind": "user", "content": "hi"})
    sid = rt.start_session(Sequence(WaitForMailbox(conversation="conv"), "consume"))
    drain(rt)

    assert store.get_session(sid)["status"] == "completed"
    assert [e["event_id"] for e in sink[0]] == ["e1"]
    # Acknowledged inside the commit: nothing claimable, nothing leased.
    assert mailbox.claimable_count("conv") == 0
    assert mailbox.claim("conv", consumer="probe") == []


def test_wait_parks_then_wakes_on_append(make_rt, mailbox, store):
    sink = []
    rt = make_rt({"consume": make_consume(sink)})
    sid = rt.start_session(Sequence(WaitForMailbox(conversation="conv"), "consume"))
    drain(rt)

    parked = waiting_execution(store, sid)
    assert parked is not None
    assert parked["wake"]["conversation"] == "conv"
    assert store.get_session(sid)["status"] == "running"
    assert rt.wake_due_waits() == 0  # no mail, no deadline — stays parked

    mailbox.append("conv", {"event_id": "e2", "kind": "user", "content": "now"})
    assert rt.wake_due_waits() == 1
    drain(rt)

    assert store.get_session(sid)["status"] == "completed"
    assert [e["event_id"] for e in sink[0]] == ["e2"]


def test_spurious_wake_reparks_with_same_deadline(make_rt, mailbox, store):
    rt = make_rt({"consume": make_consume([])})
    sid = rt.start_session(
        Sequence(WaitForMailbox(conversation="conv", timeout=60), "consume")
    )
    drain(rt)
    first = waiting_execution(store, sid)
    assert first["wake"]["wake_at"] is not None

    assert store.wake_execution(first["id"]) is True
    assert store.wake_execution(first["id"]) is False  # idempotent
    rt._enqueue_ready(sid)  # a woken wait is ready — recovery's path
    drain(rt)

    reparked = waiting_execution(store, sid)
    assert reparked is not None
    # The timeout anchors at the first park and survives re-parking.
    assert reparked["wake"]["wake_at"] == pytest.approx(
        first["wake"]["wake_at"], abs=1e-6
    )


def test_timeout_delivers_timer_event(make_rt, mailbox, store):
    sink = []
    rt = make_rt({"consume": make_consume(sink)})
    sid = rt.start_session(
        Sequence(WaitForMailbox(conversation="conv", timeout=0.3), "consume")
    )
    drain(rt, rounds=3)
    assert waiting_execution(store, sid) is not None

    time.sleep(0.35)
    assert rt.wake_due_waits() == 1
    drain(rt)

    assert store.get_session(sid)["status"] == "completed"
    (events,) = sink
    assert len(events) == 1
    assert events[0]["kind"] == "system"
    assert events[0]["source"] == "timer"


def test_mail_beats_timer_when_both_ready(make_rt, mailbox, store):
    sink = []
    rt = make_rt({"consume": make_consume(sink)})
    sid = rt.start_session(
        Sequence(WaitForMailbox(conversation="conv", timeout=0.3), "consume")
    )
    drain(rt, rounds=3)
    assert waiting_execution(store, sid) is not None
    time.sleep(0.35)
    # Deadline passed AND mail arrived before the wake ran: mail wins.
    mailbox.append("conv", {"event_id": "e3", "kind": "user", "content": "late"})
    assert rt.wake_due_waits() == 1
    drain(rt)

    assert [e["event_id"] for e in sink[0]] == ["e3"]


def test_wait_is_nonterminal_for_fanin(make_rt, mailbox, store):
    order = []

    def quick(state):
        order.append("quick")
        return state

    def joined(state):
        order.append("joined")
        return state

    rt = make_rt({"quick": quick, "joined": joined})
    sid = rt.start_session(
        Sequence(Parallel(WaitForMailbox(conversation="conv"), "quick"), "joined")
    )
    drain(rt)

    assert order == ["quick"]  # the join blocks on the parked wait
    mailbox.append("conv", {"event_id": "e4", "kind": "user"})
    rt.wake_due_waits()
    drain(rt)
    assert order == ["quick", "joined"]
    assert store.get_session(sid)["status"] == "completed"


def test_parked_wait_survives_restart(make_rt, mailbox, store):
    sink = []
    registry = {"consume": make_consume(sink)}
    rt = make_rt(registry)
    sid = rt.start_session(Sequence(WaitForMailbox(conversation="conv"), "consume"))
    drain(rt)
    assert waiting_execution(store, sid) is not None

    # A new runtime (fresh queue = the old process's queue died with it)
    # finds the parked wait in the store; the appended event wakes it
    # through the startup tick inside run().
    rt2 = QueueRuntime(
        node_registry=registry,
        store=store,
        queue=InMemoryReadyQueue(),
        mailbox=mailbox,
    )
    mailbox.append("conv", {"event_id": "e5", "kind": "user", "content": "back"})
    rt2.run(poll_wait=0.01, stop_when_idle=True)

    assert store.get_session(sid)["status"] == "completed"
    assert [e["event_id"] for e in sink[0]] == ["e5"]


def test_run_loop_sleeps_through_timeout(make_rt, mailbox, store):
    sink = []
    rt = make_rt({"consume": make_consume(sink)})
    sid = rt.start_session(
        Sequence(WaitForMailbox(conversation="conv", timeout=0.05), "consume")
    )
    # stop_when_idle must not abandon a parked timer: the loop waits it
    # out, delivers the timer event, and only then stops.
    rt.run(poll_wait=0.02, stop_when_idle=True)

    assert store.get_session(sid)["status"] == "completed"
    assert sink[0][0]["source"] == "timer"


def test_conversation_from_state(make_rt, mailbox, store):
    sink = []
    rt = make_rt({"consume": make_consume(sink)})
    mailbox.append("conv-state", {"event_id": "e6", "kind": "user"})
    sid = rt.start_session(
        Sequence(WaitForMailbox(), "consume"),
        initial_state={"conversation_id": "conv-state"},
    )
    drain(rt)
    assert store.get_session(sid)["status"] == "completed"
    assert [e["event_id"] for e in sink[0]] == ["e6"]


def test_wait_without_conversation_fails_session(make_rt, store):
    rt = make_rt({"consume": make_consume([])})
    sid = rt.start_session(Sequence(WaitForMailbox(), "consume"))
    drain(rt)
    assert store.get_session(sid)["status"] == "failed"


def test_wait_without_mailbox_fails_session(store):
    rt = QueueRuntime(
        node_registry={"consume": lambda s: s},
        store=store,
        queue=InMemoryReadyQueue(),
    )
    sid = rt.start_session(Sequence(WaitForMailbox(conversation="conv"), "consume"))
    drain(rt)
    assert store.get_session(sid)["status"] == "failed"


def test_waiting_conversations_view(make_rt, mailbox, store):
    rt = make_rt({"consume": make_consume([])})
    rt.start_session(Sequence(WaitForMailbox(conversation="conv-a"), "consume"))
    drain(rt)
    assert rt.waiting_conversations() == {"conv-a"}
