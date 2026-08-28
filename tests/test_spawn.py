"""Spawn riding the commit: child sessions, lineage, results, death notices.

Spawn is a Transition field committed atomically with the parent node's
completion; child identity is deterministic (exec id + slot) so replay
re-spawns idempotently. The engine fulfils the child contract: a finishing
child publishes a correlated kind:result event (riding END's commit
through the outbox), a failing child emits a death notice, and both feed
monitors and wake parked parents. Commit-level tests run on every store.
"""

import time

import pytest

from entourage.flow import Sequence, WaitForMailbox
from entourage.ingress import correlation_conversation
from entourage.mailbox import InMemoryMailbox
from entourage.monitors import InMemoryMonitorStore, Monitor
from entourage.runtime import InMemoryGraphStore, InMemoryReadyQueue, QueueRuntime
from entourage.runtime.planner import stage_spawn
from entourage.transition import Spawn, Transition


def make_rt(registry, store=None, mailbox=None, monitors=None):
    return QueueRuntime(
        node_registry=registry,
        store=store or InMemoryGraphStore(),
        queue=InMemoryReadyQueue(),
        mailbox=mailbox or InMemoryMailbox(),
        monitors=monitors,
    )


def drain(rt, rounds=25):
    for _ in range(rounds):
        rt.run_once(poll_wait=0.01)


def child_sessions(store):
    running = store.get_running_sessions() + store.get_terminal_sessions()
    return [s for s in running if s["id"].startswith("spawn-")]


# ── Commit-level semantics (every store) ──────────────────────


def test_commit_creates_child_idempotently(store):
    registry = {"work": lambda s: s}
    sid = store.create_session("t", {"x": 1})
    exec_id = store.add_execution(sid, "dispatch")
    child = stage_spawn(
        Spawn(plan="work", initial_state={"job": "a"}),
        exec_id, 0, registry, parent_session_id=sid,
    )

    store.commit_transition(exec_id, sid, {"done": True}, spawns=[child])
    # Replay of the same commit must not twin the child or its graph.
    store.commit_transition(exec_id, sid, {"done": True}, spawns=[child])

    created = store.get_session(child.session_id)
    assert created is not None and created["status"] == "running"
    assert created["initial_state"]["spawn"]["parent_exec_id"] == exec_id
    executions = store.get_session_executions(child.session_id)
    assert len(executions) == 3  # HEAD + work + END, no twins
    head = store.get_execution(child.head_id)
    assert head["status"] == "completed"
    (ready,) = store.get_ready_executions(child.session_id)
    assert ready["node_name"] == "work"


def test_staging_is_deterministic(store):
    registry = {"work": lambda s: s}
    a = stage_spawn(Spawn(plan="work"), "exec1", 0, registry)
    b = stage_spawn(Spawn(plan="work"), "exec1", 0, registry)
    assert a.session_id == b.session_id
    assert [ex["exec_id"] for ex in a.executions] == [
        ex["exec_id"] for ex in b.executions
    ]
    assert a.edges == b.edges


# ── Engine verticals ──────────────────────────────────────────


def test_spawned_child_runs_and_reports(store=None):
    ran = []

    def dispatch(state):
        return Transition(
            state={**state, "dispatched": True},
            spawn=[Spawn(plan="work", initial_state={"job": "resize"},
                         correlation_id="op-7")],
        )

    def work(state):
        ran.append(state)
        return {**state, "output": "done"}

    mailbox = InMemoryMailbox()
    rt = make_rt({"dispatch": dispatch, "work": work}, mailbox=mailbox)
    sid = rt.start_session(Sequence("dispatch"))
    drain(rt)

    # Parent and child both completed; lineage reached the child's input.
    assert rt.store.get_session(sid)["status"] == "completed"
    (child,) = child_sessions(rt.store)
    assert child["status"] == "completed"
    assert ran[0]["spawn"]["correlation_id"] == "op-7"

    # The child contract, engine-fulfilled: a correlated result event in
    # the default derived conversation.
    conv = correlation_conversation("op-7")
    (event,) = mailbox.claim(conv, consumer="probe")
    assert event["kind"] == "result"
    assert event["correlation_id"] == "op-7"
    assert event["payload"]["output"] == "done"
    assert event["event_id"] == f"result-{child['id']}"


def test_fork_join_via_wait(store=None):
    joined = []

    def dispatch(state):
        return Transition(
            state=state,
            plan=Sequence(
                WaitForMailbox(conversation=correlation_conversation("op-8")),
                "consume",
            ),
            spawn=[Spawn(plan="work", correlation_id="op-8")],
        )

    def work(state):
        return {**state, "answer": 42}

    def consume(state):
        joined.append(state["events"])
        return state

    rt = make_rt({"dispatch": dispatch, "work": work, "consume": consume})
    sid = rt.start_session(Sequence("dispatch"))
    drain(rt)

    assert rt.store.get_session(sid)["status"] == "completed"
    (events,) = joined
    assert events[0]["kind"] == "result"
    assert events[0]["payload"]["answer"] == 42


def test_failed_child_emits_death_notice_and_feeds_monitor():
    def dispatch(state):
        return Transition(
            state=state,
            spawn=[Spawn(plan="explode", correlation_id="op-9", notify="inbox")],
            arm=[Monitor(notify="inbox", correlation_id="op-9",
                         deadline=time.time() + 60)],
        )

    def explode(state):
        raise RuntimeError("boom")

    mailbox = InMemoryMailbox()
    monitors = InMemoryMonitorStore()
    rt = make_rt({"dispatch": dispatch, "explode": explode},
                 mailbox=mailbox, monitors=monitors)
    rt.start_session(Sequence("dispatch"))
    drain(rt)

    (child,) = child_sessions(rt.store)
    assert child["status"] == "failed"
    (event,) = mailbox.claim("inbox", consumer="probe")
    assert event["kind"] == "system"
    assert event["correlation_id"] == "op-9"
    assert event["payload"]["reason"] == "failed"
    assert "boom" in event["payload"]["error"]
    # The death notice is an observation too: the deadline monitor is
    # satisfied (silence did not happen) instead of firing a second alarm.
    assert monitors.list() == []


def test_supervision_inbox_and_multiple_children():
    seen = []

    def dispatch(state):
        return Transition(
            state=state,
            plan=Sequence(WaitForMailbox(conversation="inbox"), "consume"),
            spawn=[
                Spawn(plan="work_a", slot="a", notify="inbox"),
                Spawn(plan="work_b", slot="b", notify="inbox"),
            ],
        )

    def consume(state):
        seen.extend(state["events"])
        return state

    rt = make_rt({
        "dispatch": dispatch,
        "work_a": lambda s: {**s, "who": "a"},
        "work_b": lambda s: {**s, "who": "b"},
        "consume": consume,
    })
    sid = rt.start_session(Sequence("dispatch"))
    drain(rt)

    assert rt.store.get_session(sid)["status"] == "completed"
    assert len(child_sessions(rt.store)) == 2
    # At least one child's result reached the supervisor's single drain;
    # both were published to the inbox (the drain may batch them).
    assert all(e["kind"] == "result" for e in seen)
    assert {e["payload"]["who"] for e in seen} <= {"a", "b"}
    assert len(seen) >= 1


def test_spawn_without_mailbox_fails_node():
    def dispatch(state):
        return Transition(state=state, spawn=[Spawn(plan="work")])

    rt = QueueRuntime(
        node_registry={"dispatch": dispatch, "work": lambda s: s},
        store=InMemoryGraphStore(),
        queue=InMemoryReadyQueue(),
    )
    sid = rt.start_session(Sequence("dispatch"))
    drain(rt)
    assert rt.store.get_session(sid)["status"] == "failed"
    assert child_sessions(rt.store) == []
