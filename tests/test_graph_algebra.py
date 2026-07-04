"""
Graph-algebra tests: plan expansion, ready detection, joins, gates, and
dynamic plan injection — parametrized over GraphStore and ReadyQueue
backends (fixtures in conftest.py) so every backend passes the same suite.
"""

from entourage.flow import Conditional, Parallel, Sequence
from entourage.runtime import Runtime
from entourage.runtime.planner import resolve_node


class Recorder:
    """Builds nodes that record call order and inputs, and tag the state."""

    def __init__(self):
        self.calls = []
        self.inputs = {}

    def node(self, name, plan=None, extra=None):
        def fn(state):
            self.calls.append(name)
            self.inputs[name] = state
            new_state = {**state, name: True, **(extra or {})}
            return new_state, plan
        return fn

    def registry(self, *names):
        return {n: self.node(n) for n in names}


def run_to_completion(runtime):
    runtime.run(poll_wait=0.01, stop_when_idle=True)


# ── Plan expansion & execution ────────────────────────────────


def test_single_node(store, make_runtime):
    rec = Recorder()
    rt = make_runtime(store, rec.registry("a"))
    sid = rt.start_session("t", {"seed": 1}, plan="a")
    run_to_completion(rt)

    assert rec.calls == ["a"]
    assert rec.inputs["a"] == {"seed": 1}
    assert store.get_session(sid)["status"] == "completed"


def test_sequence_order_and_state_threading(store, make_runtime):
    rec = Recorder()
    rt = make_runtime(store, rec.registry("a", "b", "c"))
    sid = rt.start_session("t", {"seed": 1}, plan=Sequence("a", "b", "c"))
    run_to_completion(rt)

    assert rec.calls == ["a", "b", "c"]
    # each node sees the accumulated state of its predecessors
    assert rec.inputs["c"] == {"seed": 1, "a": True, "b": True}
    assert store.get_session(sid)["status"] == "completed"


def test_parallel_fork_join(store, make_runtime):
    rec = Recorder()
    rt = make_runtime(store, rec.registry("a", "b", "c", "d"))
    sid = rt.start_session(
        "t", {}, plan=Sequence("a", Parallel("b", "c"), "d")
    )
    run_to_completion(rt)

    assert set(rec.calls) == {"a", "b", "c", "d"}
    assert rec.calls[0] == "a"
    assert rec.calls[-1] == "d"
    # join: d sees the merged result of both branches
    assert rec.inputs["d"]["b"] is True
    assert rec.inputs["d"]["c"] is True
    assert store.get_session(sid)["status"] == "completed"


def test_nested_parallel_branches(store, make_runtime):
    rec = Recorder()
    rt = make_runtime(store, rec.registry("a", "b1", "b2", "c", "d"))
    sid = rt.start_session(
        "t", {}, plan=Sequence("a", Parallel(Sequence("b1", "b2"), "c"), "d")
    )
    run_to_completion(rt)

    assert rec.calls.index("b1") < rec.calls.index("b2")
    assert rec.calls[-1] == "d"
    assert rec.inputs["d"]["b2"] is True
    assert rec.inputs["d"]["c"] is True
    assert store.get_session(sid)["status"] == "completed"


def test_join_waits_for_all_parents(store, make_runtime):
    """The merge node must not be ready until every branch completed."""
    rec = Recorder()
    rt = make_runtime(store, rec.registry("slow1", "slow2", "after"))
    rt.start_session("t", {}, plan=Sequence(Parallel("slow1", "slow2"), "after"))

    # Drain step by step: 'after' must never run before both branches
    for _ in range(20):
        rt.run_once(poll_wait=0.01)
        if "after" in rec.calls:
            break
    assert rec.calls[-1] == "after"
    assert {"slow1", "slow2"} <= set(rec.calls[:-1])


# ── Conditionals / gates ──────────────────────────────────────


def test_conditional_taken(store, make_runtime):
    rec = Recorder()
    registry = rec.registry("x")
    registry["triage"] = rec.node("triage", extra={"go": True})
    rt = make_runtime(store, registry)
    sid = rt.start_session("t", {}, plan=Sequence("triage", Conditional("go", "x")))
    run_to_completion(rt)

    assert rec.calls == ["triage", "x"]
    assert store.get_session(sid)["status"] == "completed"


def test_conditional_skipped(store, make_runtime):
    rec = Recorder()
    registry = rec.registry("x")
    registry["triage"] = rec.node("triage", extra={"go": False})
    rt = make_runtime(store, registry)
    sid = rt.start_session("t", {}, plan=Sequence("triage", Conditional("go", "x")))
    run_to_completion(rt)

    assert rec.calls == ["triage"]  # gated branch never ran
    assert store.get_session(sid)["status"] == "completed"


# ── Dynamic plan injection (Control-by-Return) ────────────────


def test_dynamic_plan_injection(store, make_runtime):
    rec = Recorder()
    registry = rec.registry("a", "b")
    registry["planner"] = rec.node("planner", plan=Sequence("a", "b"))
    rt = make_runtime(store, registry)
    sid = rt.start_session("t", {}, plan="planner")
    run_to_completion(rt)

    assert rec.calls == ["planner", "a", "b"]
    # injected nodes see the planner's state, downstream sees theirs
    assert rec.inputs["a"]["planner"] is True
    assert store.get_session(sid)["status"] == "completed"


def test_dynamic_injection_preserves_successors(store, make_runtime):
    rec = Recorder()
    registry = rec.registry("injected", "after")
    registry["planner"] = rec.node("planner", plan="injected")
    rt = make_runtime(store, registry)
    sid = rt.start_session("t", {}, plan=Sequence("planner", "after"))
    run_to_completion(rt)

    # 'after' runs after the injected plan, not in parallel with it
    assert rec.calls == ["planner", "injected", "after"]
    assert rec.inputs["after"]["injected"] is True
    assert store.get_session(sid)["status"] == "completed"


def test_rewiring_edges(store, make_runtime):
    """After injection: planner → injected → original children."""
    rec = Recorder()
    registry = rec.registry("injected", "after")
    registry["planner"] = rec.node("planner", plan="injected")
    rt = make_runtime(store, registry)
    sid = rt.start_session("t", {}, plan=Sequence("planner", "after"))
    run_to_completion(rt)

    graph = store.get_session_graph(sid)
    by_name = {ex["id"]: ex["node_name"] for ex in graph["executions"]}
    edges = {
        (by_name[e["from_exec_id"]], by_name[e["to_exec_id"]])
        for e in graph["edges"]
    }
    assert ("planner", "injected") in edges
    assert ("injected", "after") in edges
    assert ("planner", "after") not in edges


# ── Failure & recovery ────────────────────────────────────────


def test_node_failure_marks_execution_failed(store, make_runtime):
    def boom(state):
        raise RuntimeError("kaput")

    rt = make_runtime(store, {"boom": boom})
    sid = rt.start_session("t", {}, plan="boom")
    run_to_completion(rt)

    failed = store.get_session_executions(sid, status="failed")
    assert len(failed) == 1
    assert failed[0]["node_name"] == "boom"
    assert "kaput" in failed[0]["result_state"]["error"]
    # default policy is single-attempt: terminal failure fails the session
    assert failed[0]["attempts"] == 1
    assert store.get_session(sid)["status"] == "failed"


def test_crash_recovery_resumes_ready_nodes(store, make_runtime):
    rec = Recorder()
    rt = make_runtime(store, rec.registry("a", "b", "c"))
    sid = rt.start_session("t", {}, plan=Sequence("a", "b", "c"))
    rt.run_once(poll_wait=0.01)  # executes 'a', enqueues 'b'
    assert rec.calls == ["a"]

    # "Crash": the queue's in-flight contents are lost; only the store survives.
    rt2 = make_runtime(store, rec.registry("a", "b", "c"))
    run_to_completion(rt2)  # startup recovery re-enqueues ready nodes

    assert rec.calls[-2:] == ["b", "c"]
    assert store.get_session(sid)["status"] == "completed"


def test_unknown_node_fails_execution(store, make_runtime):
    rt = make_runtime(store, {})
    sid = rt.start_session("t", {}, plan="ghost")
    run_to_completion(rt)

    failed = store.get_session_executions(sid, status="failed")
    assert [f["node_name"] for f in failed] == ["ghost"]
    assert store.get_session(sid)["status"] == "failed"


# ── Callable nodes & the local Runtime facade ─────────────────


def test_callable_nodes_autoregister():
    calls = []

    def b(state):
        calls.append("b")
        return {**state, "b": True}, None

    def a(state):
        calls.append("a")
        return {**state, "a": True}, b  # returns a dynamic plan: the callable

    rt = Runtime()
    sid = rt.start_session(a, {"seed": 1})
    rt.run()

    assert calls == ["a", "b"]
    assert rt.store.get_session(sid)["status"] == "completed"


def test_callable_instance_nodes():
    class Agent:
        def __init__(self):
            self.calls = 0

        def __call__(self, state):
            self.calls += 1
            return {**state, "agent": self.calls}, None

    agent = Agent()
    rt = Runtime()
    sid = rt.start_session(Sequence(agent, agent), {})
    rt.run()

    assert agent.calls == 2
    assert rt.store.get_session(sid)["status"] == "completed"


def test_resolve_node_registry_semantics():
    registry = {}

    def foo(state):
        return state, None

    name1 = resolve_node(foo, registry)
    name2 = resolve_node(foo, registry)  # same object → same name
    assert name1 == name2 == "foo"
    assert registry == {"foo": foo}

    other = lambda state: (state, None)
    other.__name__ = "foo"  # different object, colliding name
    name3 = resolve_node(other, registry)
    assert name3 != "foo"
    assert registry[name3] is other

    assert resolve_node("bar", registry) == "bar"  # strings pass through
