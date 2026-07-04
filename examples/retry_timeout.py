"""
Retry & timeout controls on nodes — no external services needed.

Wrap any plan leaf in ``flow.Node(...)`` to attach an execution policy:

    Node("call_api", max_attempts=4, timeout=2.0, retry_delay=0.3)

- ``max_attempts``: total tries before the execution — and its session —
  is marked failed (default 1, i.e. no retry);
- ``timeout``: wall-clock seconds per attempt, a timed-out attempt counts
  as a failed one;
- ``retry_delay``: seconds to hold a failed attempt back before re-enqueue.

The policy is stored on the execution in the GraphStore, so in a
multi-worker deployment every worker honors it no matter which worker
expanded the plan. The same policy can also be registered as a per-name
default: ``runtime.register_node("call_api", fn, max_attempts=3)``.

Run:  python examples/retry_timeout.py
"""

import logging
import time

from entourage.flow import Node, Sequence
from entourage.runtime import Runtime

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")


# ── Nodes ─────────────────────────────────────────────────────

class FlakyAPI:
    """Simulates a transient outage: the first two calls raise."""

    def __init__(self):
        self.calls = 0

    def __call__(self, state):
        self.calls += 1
        if self.calls <= 2:
            print(f"    flaky_api: attempt {self.calls} → ConnectionError (transient)")
            raise ConnectionError("upstream reset the connection")
        print(f"    flaky_api: attempt {self.calls} → 200 OK")
        return {**state, "data": "payload"}, None


def hanging_call(state):
    """Simulates a call that never returns (e.g. a wedged C extension)."""
    print("    hanging_call: started, will hang...")
    time.sleep(60)
    return state, None


def report(state):
    print(f"    report: got state {state}")
    return {**state, "reported": True}, None


def show(store, session_id, title):
    print(f"\n  {title}")
    session = store.get_session(session_id)
    print(f"  session status: {session['status']}")
    for ex in store.get_session_executions(session_id):
        if ex["node_name"].startswith("__"):
            continue  # skip HEAD/END sentinels
        err = f", last_error={ex['last_error']!r}" if ex["last_error"] else ""
        print(
            f"    {ex['node_name']}: {ex['status']} "
            f"(attempts={ex['attempts']}{err})"
        )


# ── 1. Transient failure, absorbed by retries ─────────────────

print("═══ 1. flaky API with max_attempts=4, retry_delay=0.3 ═══")
flaky = FlakyAPI()
rt = Runtime()
sid = rt.start_session(
    Sequence(
        Node(flaky, max_attempts=4, retry_delay=0.3),
        report,
    ),
    {"query": "hello"},
)
t0 = time.time()
rt.run()
show(rt.store, sid, f"finished in {time.time() - t0:.2f}s (two 0.3s retry delays)")

# ── 2. Hung node, cut off by timeout; retries exhausted ───────

print("\n═══ 2. hanging call with timeout=0.5, max_attempts=2 ═══")
rt = Runtime()
sid = rt.start_session(
    Sequence(
        Node(hanging_call, max_attempts=2, timeout=0.5),
        report,  # never reached: exhaustion fails the session
    ),
    {},
)
rt.run()
show(rt.store, sid, "both attempts timed out → terminal failure")
print("\n  note: report never ran — a terminally failed node fails its session.")
