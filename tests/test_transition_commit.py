"""Atomic transition commit — a node's returned (state, plan) lands as one unit.

Before ``commit_transition``, the runtime applied a node's outcome as
independently durable writes (mark_completed → expand_plan → rewire). A crash
after the first write silently lost the returned plan: the node stayed
completed, so the idempotence guard correctly refused to re-run it, and the
session finished without the work the node had scheduled. These tests crash
the store mid-commit and assert that either the whole transition is visible
or none of it, and that a restart re-runs the interrupted node to completion.
"""

import pytest

from entourage.runtime import InMemoryReadyQueue, QueueRuntime


class SimulatedCrash(BaseException):
    """Passes the runtime's ``except Exception`` handlers, like a real crash."""


def make_runtime(store, registry):
    return QueueRuntime(
        node_registry=registry,
        store=store,
        queue=InMemoryReadyQueue(),
        retention_policy=None,
    )


def execs_by_name(store, session_id, name):
    return [
        ex
        for ex in store.get_session_executions(session_id)
        if ex["node_name"] == name
    ]


def arm_commit_crash(store):
    """Sabotage the store so the next transition commit dies mid-flight.

    Returns a disarm callable. SQLite: the base commit's first primitive
    (``add_execution``) raises inside the transaction scope — rollback must
    discard the whole write-set. Redis: the commit pipeline's ``execute``
    raises — the buffered write-set must never reach the server.
    """
    kind = type(store).__name__
    if kind == "SQLiteGraphStore":
        def boom(*args, **kwargs):
            raise SimulatedCrash()

        store.add_execution = boom
        return lambda: store.__dict__.pop("add_execution", None)

    if kind == "RedisGraphStore":
        real_pipeline = store._r.pipeline

        def crashing_pipeline(*args, **kwargs):
            pipe = real_pipeline(*args, **kwargs)

            def boom(*a, **k):
                raise SimulatedCrash()

            pipe.execute = boom
            return pipe

        store._r.pipeline = crashing_pipeline

        def disarm():
            try:
                del store._r.pipeline
            except AttributeError:
                pass

        return disarm

    raise AssertionError(f"no crash injection for {kind}")


def test_returned_plan_runs_and_completes(store):
    ran = []

    def planner(state):
        return {**state, "planned": True}, "follow"

    def follow(state):
        ran.append(True)
        return {**state, "followed": True}

    rt = make_runtime(store, {"planner": planner, "follow": follow})
    sid = rt.start_session("test", {}, plan="planner")
    rt.run(poll_wait=0.2, stop_when_idle=True)

    assert ran == [True]
    assert store.get_session(sid)["status"] == "completed"
    [follow_ex] = execs_by_name(store, sid, "follow")
    assert follow_ex["status"] == "completed"
    assert follow_ex["result_state"]["planned"] is True


def test_crash_mid_commit_commits_nothing(store):
    if type(store).__name__ == "InMemoryGraphStore":
        pytest.skip(
            "base commit is documented non-atomic; the in-memory store "
            "cannot survive a real crash anyway"
        )

    calls = {"planner": 0}
    disarm = [None]

    def planner(state):
        calls["planner"] += 1
        if calls["planner"] == 1:
            # Arm after mark_running, so the next store write is the commit.
            disarm[0] = arm_commit_crash(store)
        return {**state, "planned": calls["planner"]}, "follow"

    def follow(state):
        return {**state, "followed": True}

    registry = {"planner": planner, "follow": follow}
    rt = make_runtime(store, registry)
    sid = rt.start_session("test", {}, plan="planner")
    try:
        with pytest.raises(SimulatedCrash):
            rt.run(poll_wait=0.2, stop_when_idle=True)
    finally:
        if disarm[0]:
            disarm[0]()

    # Nothing of the transition landed: the node is still running, no
    # trace of the returned plan, and the graph shape is unchanged.
    [planner_ex] = execs_by_name(store, sid, "planner")
    assert planner_ex["status"] == "running"
    assert planner_ex["result_state"] is None
    assert execs_by_name(store, sid, "follow") == []

    # "Reboot": a fresh runtime over the same store recovers the session,
    # re-runs the interrupted node, and the plan it returns is honored.
    rt2 = make_runtime(store, registry)
    rt2.run(poll_wait=0.2, stop_when_idle=True)

    assert calls["planner"] == 2
    assert store.get_session(sid)["status"] == "completed"
    [planner_ex] = execs_by_name(store, sid, "planner")
    assert planner_ex["status"] == "completed"
    [follow_ex] = execs_by_name(store, sid, "follow")
    assert follow_ex["status"] == "completed"


def test_crashed_node_reruns_after_restart(store):
    calls = {"flaky": 0}

    def flaky(state):
        calls["flaky"] += 1
        if calls["flaky"] == 1:
            raise SimulatedCrash()
        return {**state, "ok": True}, "follow"

    def follow(state):
        return {**state, "followed": True}

    registry = {"flaky": flaky, "follow": follow}
    rt = make_runtime(store, registry)
    sid = rt.start_session("test", {}, plan="flaky")
    with pytest.raises(SimulatedCrash):
        rt.run(poll_wait=0.2, stop_when_idle=True)

    [flaky_ex] = execs_by_name(store, sid, "flaky")
    assert flaky_ex["status"] == "running"

    rt2 = make_runtime(store, registry)
    rt2.run(poll_wait=0.2, stop_when_idle=True)

    assert calls["flaky"] == 2
    assert store.get_session(sid)["status"] == "completed"
    [follow_ex] = execs_by_name(store, sid, "follow")
    assert follow_ex["status"] == "completed"


def test_duplicate_delivery_is_noop(store):
    def planner(state):
        return {**state, "planned": True}, "follow"

    def follow(state):
        return {**state, "followed": True}

    rt = make_runtime(store, {"planner": planner, "follow": follow})
    sid = rt.start_session("test", {}, plan="planner")
    rt.run(poll_wait=0.2, stop_when_idle=True)

    [planner_ex] = execs_by_name(store, sid, "planner")
    assert len(execs_by_name(store, sid, "follow")) == 1

    # At-least-once transports can redeliver an already-committed execution.
    rt.queue.send({
        "type": "execute",
        "exec_id": planner_ex["id"],
        "session_id": sid,
        "time_created": 0,
    })
    rt.run_once(poll_wait=0.1)

    assert len(execs_by_name(store, sid, "follow")) == 1
    [planner_after] = execs_by_name(store, sid, "planner")
    assert planner_after["result_state"] == planner_ex["result_state"]


def test_invalid_plan_fails_before_any_commit(store):
    def bad(state):
        return {**state}, 12345  # not a valid plan

    rt = make_runtime(store, {"bad": bad})
    sid = rt.start_session("test", {}, plan="bad")
    rt.run(poll_wait=0.2, stop_when_idle=True)

    # Staging fails before the commit, so the node fails cleanly instead of
    # the old completed-then-overwritten-as-failed sequence.
    [bad_ex] = execs_by_name(store, sid, "bad")
    assert bad_ex["status"] == "failed"
    assert store.get_session(sid)["status"] == "failed"
