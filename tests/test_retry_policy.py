"""
Retry & timeout policy tests — parametrized over GraphStore and ReadyQueue
backends (fixtures in conftest.py).

Semantics under test:
- a node wrapped in ``flow.Node(max_attempts=N)`` is re-run until it
  succeeds or N attempts are spent; exhaustion is terminal and fails the
  session;
- ``timeout`` bounds each attempt's wall-clock time, and a timed-out
  attempt counts as a failed one;
- ``retry_delay`` holds a failed execution back before redelivery;
- policies attach per-leaf (``flow.Node``) or per-name
  (``register_node``), leaf fields winning.
"""

import time

from entourage.flow import Node, Sequence
from entourage.runtime import Runtime


def run(rt):
    rt.run(poll_wait=0.01, stop_when_idle=True)


class Flaky:
    """Raises on the first ``failures`` calls, then succeeds."""

    def __init__(self, failures: int):
        self.failures = failures
        self.call_times = []

    @property
    def calls(self):
        return len(self.call_times)

    def __call__(self, state):
        self.call_times.append(time.time())
        if self.calls <= self.failures:
            raise RuntimeError(f"transient #{self.calls}")
        return {**state, "ok": True}, None


def get_exec(store, sid, node_name):
    (ex,) = [
        e
        for e in store.get_session_executions(sid)
        if e["node_name"] == node_name
    ]
    return ex


def test_flaky_node_recovers_and_state_flows_on(store, make_runtime):
    flaky = Flaky(2)
    seen = {}

    def after(state):
        seen.update(state)
        return state, None

    rt = make_runtime(store, {"flaky": flaky, "after": after})
    sid = rt.start_session(
        "t", {"seed": 1}, plan=Sequence(Node("flaky", max_attempts=3), "after")
    )
    run(rt)

    assert flaky.calls == 3
    ex = get_exec(store, sid, "flaky")
    assert ex["status"] == "completed"
    assert ex["attempts"] == 3
    assert "transient" in ex["last_error"]  # trace of the failed attempts
    # downstream sees the successful attempt's result
    assert seen == {"seed": 1, "ok": True}
    assert store.get_session(sid)["status"] == "completed"


def test_exhausted_retries_fail_session_terminally(store, make_runtime):
    flaky = Flaky(99)
    ran_after = []

    rt = make_runtime(
        store, {"flaky": flaky, "after": lambda s: (ran_after.append(1) or s, None)}
    )
    sid = rt.start_session(
        "t", {}, plan=Sequence(Node("flaky", max_attempts=2), "after")
    )
    run(rt)

    assert flaky.calls == 2
    ex = get_exec(store, sid, "flaky")
    assert ex["status"] == "failed"
    assert ex["attempts"] == 2
    assert "transient #2" in ex["result_state"]["error"]
    assert ran_after == []  # downstream never ran
    assert store.get_session(sid)["status"] == "failed"


def test_timeout_attempt_counts_as_failure_then_recovers(store, make_runtime):
    calls = []

    def sometimes_slow(state):
        calls.append(1)
        if len(calls) == 1:
            time.sleep(0.5)
        return {**state, "ok": True}, None

    rt = make_runtime(store, {"slow": sometimes_slow})
    sid = rt.start_session("t", {}, plan=Node("slow", max_attempts=2, timeout=0.1))
    run(rt)

    assert len(calls) == 2
    ex = get_exec(store, sid, "slow")
    assert ex["status"] == "completed"
    assert "timed out" in ex["last_error"]
    assert store.get_session(sid)["status"] == "completed"


def test_timeout_exhaustion_is_terminal(store, make_runtime):
    def always_slow(state):
        time.sleep(0.3)
        return state, None

    rt = make_runtime(store, {"slow": always_slow})
    sid = rt.start_session("t", {}, plan=Node("slow", max_attempts=2, timeout=0.05))
    run(rt)

    ex = get_exec(store, sid, "slow")
    assert ex["status"] == "failed"
    assert ex["attempts"] == 2
    assert "timed out" in ex["result_state"]["error"]
    assert store.get_session(sid)["status"] == "failed"


def test_retry_delay_holds_back_redelivery(store, make_runtime):
    flaky = Flaky(1)
    rt = make_runtime(store, {"flaky": flaky})
    sid = rt.start_session(
        "t", {}, plan=Node("flaky", max_attempts=2, retry_delay=0.2)
    )
    run(rt)

    assert flaky.calls == 2
    assert flaky.call_times[1] - flaky.call_times[0] >= 0.2
    assert store.get_session(sid)["status"] == "completed"


def test_policy_via_register_node(store, make_runtime):
    flaky = Flaky(1)
    rt = make_runtime(store, {})
    rt.register_node("flaky", flaky, max_attempts=3)
    sid = rt.start_session("t", {}, plan="flaky")
    run(rt)

    assert flaky.calls == 2
    assert store.get_session(sid)["status"] == "completed"


def test_leaf_policy_overrides_registered_default(store, make_runtime):
    flaky = Flaky(99)
    rt = make_runtime(store, {})
    rt.register_node("flaky", flaky, max_attempts=5)
    sid = rt.start_session("t", {}, plan=Node("flaky", max_attempts=2))
    run(rt)

    assert flaky.calls == 2  # leaf's 2, not the registered 5
    assert store.get_session(sid)["status"] == "failed"


def test_node_wrapper_autoregisters_callables():
    flaky = Flaky(1)
    rt = Runtime()
    sid = rt.start_session(Node(flaky, max_attempts=2), {})
    rt.run()

    assert flaky.calls == 2
    assert rt.store.get_session(sid)["status"] == "completed"
