"""Pending exchanges in state: matching, joins, interruptibility, examples."""

from pathlib import Path
import subprocess
import sys

import pytest

from entourage.exchanges import Exchanges
from entourage.executables import Context, Dispatcher, Executable
from entourage.runner import notify_failures
from entourage.sessions import LocalSessions


ROOT = Path(__file__).resolve().parents[1]


def ids(mail):
    return [event["event_id"] for event in mail]


def test_replies_match_on_request_id_and_expected_sender():
    state = {}
    context = Context("parent", "p:v1", {})
    exchanges = Exchanges(state)
    request_id = exchanges.request(context, "tool", {"q": 1}, key="q:1", label="lookup")
    assert state["exchanges"] == {request_id: {"to": "tool", "label": "lookup"}}

    forged = {"event_id": "f", "kind": "result", "request_id": request_id, "source": "other",
              "payload": {}}
    steering = {"event_id": "s", "kind": "user", "payload": {"text": "skip the hotel"}}
    real = {"event_id": "r", "kind": "result", "request_id": request_id, "source": "tool",
            "payload": {"answer": 42}}
    replies, others = Exchanges(state).ingest([forged, steering, real])
    assert others == [forged, steering]
    assert [(r.label, r.source, r.payload) for r in replies] == [("lookup", "tool", {"answer": 42})]
    assert not Exchanges(state)


def test_drop_forgets_exchanges_to_one_destination():
    state = {}
    context = Context("parent", "p:v1", {})
    exchanges = Exchanges(state)
    exchanges.request(context, "a", {}, key="a1")
    exchanges.request(context, "a", {}, key="a2", label="second")
    exchanges.request(context, "b", {}, key="b1")
    assert sorted(exchanges.drop("a")) == ["a1", "second"]
    assert [p["to"] for p in exchanges.pending.values()] == ["b"]


@pytest.fixture
def clock():
    return [100.0]


@pytest.fixture
def store(tmp_path, clock):
    return LocalSessions(tmp_path / "sessions.db", clock=lambda: clock[0])


def test_parent_joins_children_and_takes_steering_while_waiting(store):
    seen = []

    def parent(ctx, state, mail):
        exchanges = Exchanges(state)
        if not state.get("started"):
            state["started"] = True
            for slot in ("a", "b"):
                exchanges.call(ctx, "child:v1", {"slot": slot}, {"slot": slot}, key=slot)
            return ctx.propose(state, incorporated=ids(mail))
        replies, others = exchanges.ingest(mail)
        seen.append(([r.label for r in replies], [e["event_id"] for e in others]))
        state.setdefault("results", []).extend(r.payload["slot"] for r in replies)
        return ctx.propose(state, incorporated=ids(mail), complete=not exchanges)

    def child(ctx, state, mail):
        if state["slot"] == "b" and not state.get("asked"):
            # b answers only after being told to; keeps the parent waiting
            return ctx.propose({**state, "asked": True, "request": mail[0]}, incorporated=ids(mail))
        request = state.get("request") or mail[0]
        ctx.reply(request, {"slot": state["slot"]})
        return ctx.propose(state, incorporated=ids(mail), complete=True)

    worker = (Dispatcher(store).register(Executable("parent:v1", parent))
              .register(Executable("child:v1", child)))
    worker.create("p", "parent:v1", {})
    worker.run_until_idle()
    assert store.inspect("p")["status"] == "waiting"
    assert store.inspect("p:a")["status"] == "complete"
    assert sorted(store.inspect("p")["state"]["exchanges"][k]["to"]
                  for k in store.inspect("p")["state"]["exchanges"]) == ["p:b"]

    store.append("p", {"event_id": "steer", "kind": "user", "payload": {"text": "hurry"}})
    worker.run_until_idle()
    assert seen[-1] == ([], ["steer"])  # unrelated mail wakes the parent; b still pending
    assert store.inspect("p")["status"] == "waiting"

    store.append("p:b", {"event_id": "go", "kind": "user", "payload": {}})
    worker.run_until_idle()
    assert store.inspect("p")["status"] == "complete"
    assert sorted(store.inspect("p")["state"]["results"]) == ["a", "b"]
    assert all(label == [] or label == ["a"] or label == ["b"] for label, _ in seen)


def test_failure_notice_is_mail_the_supervisor_folds_into_its_join(store, clock):
    def parent(ctx, state, mail):
        exchanges = Exchanges(state)
        if not state.get("started"):
            state["started"] = True
            exchanges.call(ctx, "doomed:v1", {}, {}, key="doomed")
            return ctx.propose(state, incorporated=ids(mail))
        _, others = exchanges.ingest(mail)
        for event in others:
            if event["kind"] == "system" and event["payload"].get("failed"):
                state["dropped"] = exchanges.drop(event["payload"]["failed"])
        return ctx.propose(state, incorporated=ids(mail), complete=not exchanges)

    def doomed(ctx, state, mail):
        raise RuntimeError("boom")

    worker = (Dispatcher(store, max_attempts=1).register(Executable("parent:v1", parent))
              .register(Executable("doomed:v1", doomed)))
    worker.create("p", "parent:v1", {})
    results = worker.run_until_idle()
    assert [r.committed for r in results] == [True, False]
    assert store.inspect("p:doomed")["status"] == "ready"  # released with backoff
    clock[0] += 2
    assert worker.run_once() is None  # the claim finds attempts exhausted and parks it failed
    assert store.inspect("p:doomed")["status"] == "failed"
    assert notify_failures(store, "p") == 1
    assert notify_failures(store, "p") == 0
    worker.run_until_idle()
    assert store.inspect("p")["status"] == "complete"
    assert store.inspect("p")["state"]["dropped"] == ["doomed"]


def run_example(name):
    return subprocess.run([sys.executable, f"examples/{name}.py"], cwd=ROOT, check=True,
                          capture_output=True, text=True, timeout=30).stdout


def test_spawn_supervisor_example():
    output = run_example("spawn_supervisor")
    assert "join: child reported 'cat_64px.png'" in output
    assert "'ok' finished fine" in output
    assert "death notice for ['doomed']" in output
    assert "all children accounted for" in output
    assert "timer woke us with 'job-9' still pending" in output


def test_waiting_session_example():
    output = run_example("waiting_session")
    assert "got user event: 'my printer is on fire'" in output
    assert "[bob is waiting, holding no worker]" in output
    assert "got user event: 'nevermind, fixed it'" in output
    assert "[bob is complete]" in output
    assert "the timer woke us" in output
    assert "[carol is complete after 2 wakes]" in output
