"""The agent loop as resume activations: tools inline, checkpoint between model calls."""

import json
import sqlite3

import pytest

from entourage.executables import Dispatcher, Executable
from entourage.sessions import LocalSessions
from entourage.turn import ChatAgent


class Weather:
    schema = {"type": "function", "function": {
        "name": "weather", "parameters": {"type": "object", "properties": {"city": {"type": "string"}}}}}

    def execute(self, city):
        if city == "Atlantis":
            raise LookupError("no such city")
        return f"{city}: sunny"


def scripted(*replies):
    """A `complete` that returns the scripted replies in order and records what it saw."""
    seen = []

    def complete(messages, tools):
        seen.append([dict(m) for m in messages])
        return dict(replies[len(seen) - 1])
    complete.seen = seen
    return complete


def call(city, call_id="c1"):
    return {"id": call_id, "type": "function",
            "function": {"name": "weather", "arguments": json.dumps({"city": city})}}


def inputs(store, destination):
    with sqlite3.connect(store.path) as db:
        return [json.loads(row[0]) for row in db.execute(
            "SELECT event FROM wake_inputs WHERE session_id = ? ORDER BY seq", (destination,))]


@pytest.fixture
def store(tmp_path):
    return LocalSessions(tmp_path / "sessions.db", clock=lambda: 100.0)


def dispatcher(store, agent, **options):
    """A worker over a chat definition and a parked `ui` output session."""
    worker = Dispatcher(store, **options).register(Executable("chat:v1", agent.resume))
    worker.register(Executable("ui:v1", lambda ctx, state, mail: ctx.propose(
        state, incorporated=[e["event_id"] for e in mail])))
    worker.create("ui", "ui:v1", {})
    assert worker.run_once().session_id == "ui"  # first checkpoint; now it waits for mail
    return worker


def user(store, session, text, key):
    store.append(session, {"event_id": f"user:{key}", "kind": "user", "payload": {"text": text}})


def test_tool_round_is_checkpointed_and_steering_reaches_the_next_model_call(store):
    complete = scripted({"role": "assistant", "content": None, "tool_calls": [call("Kyoto")]},
                        {"role": "assistant", "content": "Sunny in Kyoto."})
    agent = ChatAgent(complete, [Weather()], system_prompt="Be brief.", output="ui")
    worker = dispatcher(store, agent)
    worker.create("chat", "chat:v1", {"messages": []})
    user(store, "chat", "Weather in Kyoto?", 1)

    assert worker.run_once().committed
    messages = store.inspect("chat")["state"]["messages"]
    assert [m["role"] for m in messages] == ["system", "user", "assistant", "tool"]
    assert messages[-1] == {"role": "tool", "tool_call_id": "c1", "name": "weather",
                            "content": "Kyoto: sunny"}
    assert store.inspect("chat")["status"] == "waiting"
    assert store.list_sessions(executable="chat:v1", ready=True)

    user(store, "chat", "Celsius please.", 2)  # arrives between tool round and model call
    result = worker.run_once()
    assert result.committed and result.session_id == "chat"
    assert complete.seen[1][-1] == {"role": "user", "content": "Celsius please."}
    assert complete.seen[1][0] == {"role": "system", "content": "Be brief."}
    assert [e["payload"] for e in inputs(store, "ui")] == [{"text": "Sunny in Kyoto."}]

    ui = worker.run_once()
    assert ui.committed and ui.session_id == "ui"
    assert worker.run_once() is None
    assert len(complete.seen) == 2


def test_queued_mail_joins_one_turn_and_a_fresh_dispatcher_continues(store, tmp_path):
    complete = scripted({"role": "assistant", "content": "Both answered."})
    agent = ChatAgent(complete, output="ui")
    worker = dispatcher(store, agent, max_events=1)
    worker.create("chat", "chat:v1", {"messages": []})
    user(store, "chat", "First", 1)
    user(store, "chat", "Second", 2)

    assert worker.run_once().committed  # batch of one, has_more: parked without a model call
    assert complete.seen == []
    assert [m["content"] for m in store.inspect("chat")["state"]["messages"]] == ["First"]

    again = Dispatcher(store, max_events=1).register(Executable("chat:v1", agent.resume))
    assert again.run_once().committed
    assert [m["role"] for m in complete.seen[0]] == ["user", "user"]
    assert inputs(store, "ui")[0]["payload"] == {"text": "Both answered."}


def test_tool_failure_is_returned_to_the_model_not_raised(store):
    complete = scripted({"role": "assistant", "content": None, "tool_calls": [call("Atlantis")]},
                        {"role": "assistant", "content": "Unknown city."})
    worker = dispatcher(store, ChatAgent(complete, [Weather()]))
    worker.create("chat", "chat:v1", {})
    user(store, "chat", "Atlantis?", 1)
    assert worker.run_once().committed
    assert "Tool error (LookupError)" in store.inspect("chat")["state"]["messages"][-1]["content"]
    assert worker.run_once().committed
    assert store.inspect("chat")["state"]["messages"][-1]["content"] == "Unknown city."
    assert worker.run_once() is None


def test_unknown_mail_fails_the_activation_instead_of_being_dropped(store):
    worker = dispatcher(store, ChatAgent(scripted()))
    worker.create("chat", "chat:v1", {})
    store.append("chat", {"event_id": "r1", "kind": "result", "payload": {}})
    result = worker.run_once()
    assert not result.committed and "unexpected mail 'r1'" in str(result.error)
    assert store.inspect("chat")["state"] == {}
