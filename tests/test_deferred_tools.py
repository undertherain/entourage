"""The shared tool runner's boundary: errors are outcomes, unknown mail is not lost."""

import pytest

from entourage.deferred_tools import Pending, ToolRunner
from entourage.executables import Dispatcher, Executable
from entourage.sessions import LocalSessions


class Faulty:
    schema = {"type": "function", "function": {"name": "faulty"}}

    def execute(self, failure):
        if failure == "lookup":
            raise KeyError("missing record")
        if failure == "encoding":
            return object()
        return Pending("another:v1", {})


@pytest.fixture
def runner(tmp_path):
    store = LocalSessions(tmp_path / "sessions.db", clock=lambda: 100.0)
    worker = Dispatcher(store, max_events=1).register(
        Executable("runner:v1", ToolRunner([Faulty()]).resume))
    worker.register(Executable("caller:v1", lambda ctx, state, mail: ctx.propose(
        state, incorporated=[e["event_id"] for e in mail])))
    worker.create("caller", "caller:v1", {})
    worker.run_until_idle()
    worker.create("work", "runner:v1", {})
    return store, worker


def request(store, name="faulty", failure="lookup"):
    store.append("work", {"event_id": "request", "kind": "request", "source": "caller",
                          "reply_to": "caller", "request_id": "request",
                          "payload": {"name": name, "arguments": {"failure": failure}}})


@pytest.mark.parametrize("name, failure, expected", [
    ("missing", "lookup", "Tool error: unknown tool 'missing'"),
    ("faulty", "lookup", "Tool error (KeyError)"),
    ("faulty", "encoding", "Tool error (TypeError)"),
    ("faulty", "nested", "a lifted tool must return a final result, not Pending"),
])
def test_tool_errors_complete_the_job_with_an_outcome(runner, name, failure, expected):
    store, worker = runner
    request(store, name, failure)
    assert worker.run_once().committed
    assert store.inspect("work")["status"] == "complete"
    # Read delivered mail without executing the caller.
    activation = store.claim(executable="caller:v1", lease_seconds=30)
    assert activation.events[0]["request_id"] == "request"
    assert activation.events[0]["source"] == "work"
    assert expected in activation.events[0]["payload"]["text"]


def test_bounded_batches_keep_the_request_but_do_not_discard_unexpected_mail(runner):
    store, worker = runner
    request(store)
    store.append("work", {"event_id": "unexpected", "kind": "steer", "payload": {"text": "change"}})
    assert worker.run_once().committed  # save the request without executing the tool yet
    assert store.inspect("work")["state"]["work"]["status"] == "working"
    result = worker.run_once()
    assert not result.committed and "unexpected mail 'unexpected'" in str(result.error)
    assert store.inspect("work")["status"] == "waiting"
    assert store.claim(executable="caller:v1", lease_seconds=30) is None
