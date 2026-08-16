"""Mailbox effects ride the transition commit (transactional outbox).

A node may return a full ``Transition``: acknowledgements of drained mailbox
events and publications to other mailboxes are recorded inside the same
atomic commit as its state and plan, applied to the mailboxes immediately
after, cleared once applied, and replayed idempotently at recovery if the
worker dies (or delivery fails) in between.
"""

import pytest

from entourage.mailbox import InMemoryMailbox
from entourage.runtime import InMemoryReadyQueue, QueueRuntime
from entourage.transition import Acknowledgement, Publication, Transition


def make_runtime(store, registry, mailbox=None, resolver=None):
    return QueueRuntime(
        node_registry=registry,
        store=store,
        queue=InMemoryReadyQueue(),
        retention_policy=None,
        mailbox=mailbox,
        mailbox_resolver=resolver,
    )


def execs_by_name(store, session_id, name):
    return [
        ex
        for ex in store.get_session_executions(session_id)
        if ex["node_name"] == name
    ]


def test_publication_lands_exactly_once(store):
    peer = InMemoryMailbox()

    def announcer(state):
        return Transition(
            state={**state, "announced": True},
            publish=[
                Publication(
                    target="peer",
                    conversation="ops",
                    event={"kind": "finding", "text": "premise looks false"},
                )
            ],
        )

    rt = make_runtime(store, {"announcer": announcer}, resolver={"peer": peer}.__getitem__)
    sid = rt.start_session("test", {}, plan="announcer")
    rt.run(poll_wait=0.2, stop_when_idle=True)

    assert store.get_session(sid)["status"] == "completed"
    [announcer_ex] = execs_by_name(store, sid, "announcer")
    assert announcer_ex["effects"] is None  # applied and cleared

    events = peer.claim("ops", consumer="test")
    assert len(events) == 1
    assert events[0]["kind"] == "finding"
    assert events[0]["event_id"].startswith("txn-")

    # An at-least-once transport redelivers the committed execution: the
    # idempotence guard skips it and nothing is published twice.
    rt.queue.send({
        "type": "execute",
        "exec_id": announcer_ex["id"],
        "session_id": sid,
        "time_created": 0,
    })
    rt.run_once(poll_wait=0.1)
    assert peer.claim("ops", consumer="test") == []
    assert peer.pending_count("ops") == 0


def test_acknowledgement_rides_commit(store):
    mailbox = InMemoryMailbox()
    event_id = mailbox.append("conv1", {"kind": "user", "text": "also check X"})
    [claimed] = mailbox.claim("conv1", consumer="worker")
    assert claimed["event_id"] == event_id

    def drainer(state):
        return Transition(
            state={**state, "ingested": [event_id]},
            acknowledge=[Acknowledgement("conv1", [event_id])],
        )

    rt = make_runtime(store, {"drainer": drainer}, mailbox=mailbox)
    sid = rt.start_session("test", {}, plan="drainer")
    rt.run(poll_wait=0.2, stop_when_idle=True)

    assert store.get_session(sid)["status"] == "completed"
    # Acknowledged: not claimable again, even after the original lease.
    assert mailbox.claim("conv1", consumer="worker") == []
    assert mailbox.pending_count("conv1") == 0


def test_failed_delivery_replays_on_recovery_without_duplicates(store):
    """Crash after commit, mid-application: first publication delivered,
    second not. The node must not fail (the transition is durable); the
    effects stay pending and recovery replays them, deduplicating the one
    that already landed."""
    peer = InMemoryMailbox()
    real_append = peer.append
    calls = {"append": 0}

    def flaky_append(conversation_id, event):
        calls["append"] += 1
        if calls["append"] == 2:
            raise ConnectionError("mailbox backend briefly down")
        return real_append(conversation_id, event)

    peer.append = flaky_append

    def announcer(state):
        return Transition(
            state={**state, "announced": True},
            publish=[
                Publication("peer", "ops", {"kind": "finding", "n": 1}),
                Publication("peer", "ops", {"kind": "finding", "n": 2}),
            ],
        )

    registry = {"announcer": announcer}
    resolver = {"peer": peer}.__getitem__
    rt = make_runtime(store, registry, resolver=resolver)
    sid = rt.start_session("test", {}, plan="announcer")
    rt.run(poll_wait=0.2, stop_when_idle=True)

    # The commit landed and the session finished; only delivery is behind.
    assert store.get_session(sid)["status"] == "completed"
    [announcer_ex] = execs_by_name(store, sid, "announcer")
    assert announcer_ex["status"] == "completed"
    assert announcer_ex["effects"] is not None  # still pending
    assert peer.pending_count("ops") == 1

    # "Reboot": recovery replays pending effects — the delivered event is
    # deduplicated by its fixed event_id, the missing one arrives.
    rt2 = make_runtime(store, registry, resolver=resolver)
    rt2.run(poll_wait=0.2, stop_when_idle=True)

    events = peer.claim("ops", consumer="test", limit=10)
    assert sorted(e["n"] for e in events) == [1, 2]
    [announcer_ex] = execs_by_name(store, sid, "announcer")
    assert announcer_ex["effects"] is None


def test_replay_skips_terminal_sessions_it_should_not_touch(store):
    """Recovery only scans running sessions; a cleanly finished session with
    cleared effects is not revisited."""
    peer = InMemoryMailbox()

    def announcer(state):
        return Transition(
            state=state,
            publish=[Publication("peer", "ops", {"kind": "finding"})],
        )

    resolver = {"peer": peer}.__getitem__
    rt = make_runtime(store, {"announcer": announcer}, resolver=resolver)
    rt.start_session("test", {}, plan="announcer")
    rt.run(poll_wait=0.2, stop_when_idle=True)
    assert peer.pending_count("ops") == 1

    rt2 = make_runtime(store, {"announcer": announcer}, resolver=resolver)
    rt2.run(poll_wait=0.2, stop_when_idle=True)
    assert peer.pending_count("ops") == 1  # no re-delivery attempt needed


def test_unresolvable_target_fails_before_commit(store):
    def bad(state):
        return Transition(
            state=state,
            publish=[Publication("nowhere", "ops", {"kind": "finding"})],
        )

    rt = make_runtime(store, {"bad": bad})
    sid = rt.start_session("test", {}, plan="bad")
    rt.run(poll_wait=0.2, stop_when_idle=True)

    [bad_ex] = execs_by_name(store, sid, "bad")
    assert bad_ex["status"] == "failed"
    assert store.get_session(sid)["status"] == "failed"


def test_transition_without_plan_or_effects_is_plain_completion(store):
    def plain(state):
        return Transition(state={**state, "done": True})

    rt = make_runtime(store, {"plain": plain})
    sid = rt.start_session("test", {}, plan="plain")
    rt.run(poll_wait=0.2, stop_when_idle=True)

    assert store.get_session(sid)["status"] == "completed"
    [plain_ex] = execs_by_name(store, sid, "plain")
    assert plain_ex["result_state"]["done"] is True
    assert plain_ex["effects"] is None


def test_force_acknowledge_is_idempotent(mailbox_backend):
    """The replay contract on both mailbox backends: force-ack ignores
    lease state, already-acknowledged events, and unknown ids."""
    mailbox = mailbox_backend
    event_id = mailbox.append("conv1", {"kind": "user", "text": "hi"})

    # Never claimed — no lease — force still acknowledges.
    mailbox.acknowledge("conv1", consumer="runtime", event_ids=[event_id], force=True)
    assert mailbox.claim("conv1", consumer="worker") == []

    # Replays: already acknowledged and unknown ids are no-ops, not errors.
    mailbox.acknowledge("conv1", consumer="runtime", event_ids=[event_id], force=True)
    mailbox.acknowledge(
        "conv1", consumer="runtime", event_ids=["missing-id"], force=True
    )

    # The strict path still guards leases.
    with pytest.raises(ValueError):
        mailbox.acknowledge("conv1", consumer="worker", event_ids=[event_id])
