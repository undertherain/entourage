"""Monitors: declared expectations armed on the commit, lapsing as mail.

Store-level tests are parametrized over both monitor backends; the
engine-level tests exercise arming via Transition effects, satisfaction
through publications and ingress, and the supervisor vertical — a parked
loop woken by a lapse event.
"""

import time
import uuid

import pytest

from entourage.flow import Sequence, WaitForMailbox
from entourage.ingress import IngressRouter, InboundEvent
from entourage.mailbox import InMemoryMailbox
from entourage.monitors import InMemoryMonitorStore, Monitor, monitor_to_dict
from entourage.runtime import InMemoryGraphStore, InMemoryReadyQueue, QueueRuntime
from entourage.transition import Publication, Transition


@pytest.fixture(params=["memory", "redis"])
def monitors(request):
    if request.param == "memory":
        yield InMemoryMonitorStore()
        return
    from entourage.monitors import RedisMonitorStore

    client = request.getfixturevalue("redis_client")
    store = RedisMonitorStore(
        client=client, namespace=f"entourage-test-monitors-{uuid.uuid4().hex[:8]}"
    )
    yield store
    store.purge()


# ── Store semantics ───────────────────────────────────────────


def deadline_record(monitor_id="m1", correlation="op-1", deadline=None):
    return {
        **monitor_to_dict(
            Monitor(
                notify="sup",
                monitor_id=monitor_id,
                correlation_id=correlation,
                deadline=deadline if deadline is not None else time.time() + 60,
            )
        )
    }


def heartbeat_record(monitor_id="h1", source="child-a", interval=60.0):
    return {
        **monitor_to_dict(
            Monitor(
                notify="sup", monitor_id=monitor_id, source=source, interval=interval
            )
        )
    }


def test_arm_is_idempotent(monitors):
    record = heartbeat_record()
    monitors.arm(record, now=100.0)
    monitors.arm(record, now=500.0)  # effect replay must not refresh
    (stored,) = monitors.list()
    assert stored["last_seen"] == 100.0


def test_deadline_satisfied_by_matching_observation(monitors):
    monitors.arm(deadline_record())
    assert monitors.observe(correlation_id="other") == 0
    assert monitors.observe(correlation_id="op-1") == 1
    assert monitors.list() == []
    assert monitors.due() == []


def test_deadline_lapses_once(monitors):
    monitors.arm(deadline_record(deadline=time.time() - 1))
    (due,) = monitors.due()
    monitors.mark_lapsed(due["monitor_id"])
    assert monitors.due() == []  # one-shot: fired, gone
    assert monitors.list() == []


def test_heartbeat_refresh_and_lapse_cycles(monitors):
    monitors.arm(heartbeat_record(interval=10.0), now=100.0)
    assert monitors.due(now=105.0) == []
    monitors.observe(source="child-a", now=108.0)  # window restarts
    assert monitors.due(now=115.0) == []
    (due,) = monitors.due(now=120.0)  # quiet past the interval
    assert due["cycles"] == 0
    monitors.mark_lapsed("h1", now=120.0)
    assert monitors.due(now=200.0) == []  # lapse fired once per quiet spell
    monitors.observe(source="child-a", now=205.0)  # resumed: re-armed
    (again,) = monitors.due(now=220.0)
    assert again["cycles"] == 1  # distinct idempotency key next lapse


def test_disarm(monitors):
    monitors.arm(deadline_record(deadline=time.time() - 1))
    assert monitors.disarm("m1") is True
    assert monitors.disarm("m1") is False
    assert monitors.due() == []


# ── Engine integration ────────────────────────────────────────


def make_rt(registry, mailbox=None, monitors=None):
    return QueueRuntime(
        node_registry=registry,
        store=InMemoryGraphStore(),
        queue=InMemoryReadyQueue(),
        mailbox=mailbox or InMemoryMailbox(),
        monitors=monitors if monitors is not None else InMemoryMonitorStore(),
    )


def drain(rt, rounds=20):
    for _ in range(rounds):
        rt.run_once(poll_wait=0.01)


def test_arming_rides_the_commit(mailbox=None):
    store = InMemoryMonitorStore()

    def dispatch(state):
        return Transition(
            state=state,
            arm=[Monitor(notify="sup", correlation_id="op-9", deadline=time.time() + 60)],
        )

    rt = make_rt({"dispatch": dispatch}, monitors=store)
    rt.start_session(Sequence("dispatch"))
    drain(rt)

    (armed,) = store.list()
    assert armed["correlation_id"] == "op-9"
    assert armed["monitor_id"].startswith("mon-")


def test_arm_without_store_fails_node():
    def dispatch(state):
        return Transition(
            state=state,
            arm=[Monitor(notify="sup", correlation_id="x", deadline=time.time() + 60)],
        )

    rt = QueueRuntime(
        node_registry={"dispatch": dispatch},
        store=InMemoryGraphStore(),
        queue=InMemoryReadyQueue(),
        mailbox=InMemoryMailbox(),
    )
    sid = rt.start_session(Sequence("dispatch"))
    drain(rt)
    assert rt.store.get_session(sid)["status"] == "failed"


def test_publication_satisfies_deadline_monitor():
    store = InMemoryMonitorStore()
    store.arm(deadline_record(correlation="op-2", deadline=time.time() - 1))

    def report(state):
        return Transition(
            state=state,
            publish=[Publication(
                target="self",
                conversation="sup",
                event={"kind": "result", "correlation_id": "op-2", "payload": {}},
            )],
        )

    rt = make_rt({"report": report}, monitors=store)
    rt.start_session(Sequence("report"))
    drain(rt)

    # Satisfied by the publication before the tick could fire the lapse.
    assert store.list() == []
    assert rt.tick_monitors() == 0


def test_lapse_wakes_parked_supervisor():
    mailbox = InMemoryMailbox()
    store = InMemoryMonitorStore()
    seen = []

    def consume(state):
        seen.append(state["events"])
        return state

    rt = make_rt({"consume": consume}, mailbox=mailbox, monitors=store)
    sid = rt.start_session(Sequence(WaitForMailbox(conversation="sup"), "consume"))
    drain(rt, rounds=3)
    assert rt.waiting_conversations() == {"sup"}

    store.arm(deadline_record(correlation="op-3", deadline=time.time() - 1))
    assert rt.tick_monitors() == 1
    assert rt.tick_monitors() == 0  # idempotent: fired once
    rt.wake_due_waits()
    drain(rt)

    assert rt.store.get_session(sid)["status"] == "completed"
    (events,) = seen
    assert events[0]["kind"] == "system"
    assert events[0]["source"] == "monitor"
    assert events[0]["payload"]["reason"] == "deadline"
    assert events[0]["payload"]["correlation_id"] == "op-3"


def test_ingress_observation_feeds_monitors():
    mailbox = InMemoryMailbox()
    store = InMemoryMonitorStore()
    rt = make_rt({}, mailbox=mailbox, monitors=store)
    store.arm(deadline_record(correlation="op-4"))

    router = IngressRouter(
        mailbox,
        default_conversation="inbox",
        observe=rt.observe_monitors,
    )
    router.accept(
        InboundEvent(
            event_id="r9", kind="result", source="remote", correlation_id="op-4"
        )
    )
    assert store.list() == []  # satisfied on arrival


def test_disarm_rides_the_commit():
    store = InMemoryMonitorStore()
    store.arm(heartbeat_record(monitor_id="h9", source="child-b"))

    def incorporate(state):
        return Transition(state=state, disarm=["h9"])

    rt = make_rt({"incorporate": incorporate}, monitors=store)
    rt.start_session(Sequence("incorporate"))
    drain(rt)
    assert store.list() == []
