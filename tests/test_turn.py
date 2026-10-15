"""The agent loop as resume activations: tools inline, checkpoint between model calls."""

import json
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from entourage.deferred_tools import Lifted, Pending, ToolRunner
from entourage.executables import Dispatcher, Executable
from entourage.runner import notify_failures
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


@pytest.mark.parametrize("lifted", [False, True], ids=["inline", "lifted"])
@pytest.mark.parametrize("city, expected", [
    ("Kyoto", "Kyoto: sunny"),
    ("Atlantis", "Tool error (LookupError): no such city"),
])
def test_inline_and_lifted_tools_deliver_the_same_outcome(store, lifted, city, expected):
    complete = scripted({"role": "assistant", "content": None, "tool_calls": [call(city)]},
                        {"role": "assistant", "content": "Done."})
    tool = Weather()
    worker = dispatcher(store, ChatAgent(complete, [Lifted(tool) if lifted else tool]))
    runner = Dispatcher(store).register(Executable("tool-runner:v1", ToolRunner([tool]).resume))
    worker.create("chat", "chat:v1", {})
    user(store, "chat", f"Weather in {city}?", 1)
    assert worker.run_once().committed
    if lifted:
        assert worker.run_once() is None  # caller releases execution until the runner replies
        assert runner.run_once().committed
        assert store.inspect("chat:c1")["status"] == "complete"
    assert worker.run_once().committed
    assert complete.seen[1][-1] == {
        "role": "tool", "tool_call_id": "c1", "name": "weather", "content": expected}
    assert worker.run_once() is None


def test_mixed_tools_wait_for_all_results_and_recover_with_a_buffered_amendment(store):
    class Clock:
        schema = {"type": "function", "function": {"name": "clock"}}

        def execute(self):
            return {"hour": 12}

    clock_call = {"id": "c3", "type": "function",
                  "function": {"name": "clock", "arguments": "{}"}}
    before = scripted({"role": "assistant", "content": None,
                       "tool_calls": [call("Kyoto", "c1"), clock_call, call("Oslo", "c2")]})
    worker = dispatcher(store, ChatAgent(before, [Lifted(Weather()), Clock()]))
    runner = Dispatcher(store).register(Executable("tool-runner:v1", ToolRunner([Weather()]).resume))
    worker.create("chat", "chat:v1", {})
    user(store, "chat", "Time and weather in Kyoto and Oslo?", 1)
    assert worker.run_once().committed
    state = store.inspect("chat")["state"]
    assert {entry["to"] for entry in state["panel"].values()} == {"chat:c1", "chat:c2"}
    assert all(not entry["interruptible"] for entry in state["panel"].values())

    user(store, "chat", "Leave Oslo for later; focus on Kyoto.", 2)
    assert worker.run_once().committed
    assert len(before.seen) == 1
    assert runner.run_once().committed  # only one of the two calls has finished
    assert worker.run_once().committed
    assert len(before.seen) == 1
    assert worker.run_once() is None

    # Reopen both sides with fresh objects; no in-memory callback or model state is needed.
    again_store = LocalSessions(store.path, clock=lambda: 100.0)
    again_runner = Dispatcher(again_store).register(
        Executable("tool-runner:v1", ToolRunner([Weather()]).resume))
    after = scripted({"role": "assistant", "content": "Noon, and sunny in Kyoto."})
    again = Dispatcher(again_store).register(
        Executable("chat:v1", ChatAgent(after, [Lifted(Weather()), Clock()], output="ui").resume))
    assert again_runner.run_once().committed
    # Put another correction after the result; a bounded batch must not call the model early.
    user(again_store, "chat", "Keep it brief.", 3)
    again.max_events = 1
    assert again.run_once().committed
    assert after.seen == []
    assert again.run_once().committed
    messages = after.seen[0]
    assert [m["role"] for m in messages] == ["user", "assistant", "tool", "tool", "tool", "user", "user"]
    assert {m["tool_call_id"]: m["content"] for m in messages if m["role"] == "tool"} == {
        "c1": "Kyoto: sunny", "c2": "Oslo: sunny", "c3": '{"hour": 12}'}
    assert [m["content"] for m in messages[-2:]] == [
        "Leave Oslo for later; focus on Kyoto.", "Keep it brief."]
    assert again_store.inspect("chat")["state"]["panel"] == {}
    assert inputs(again_store, "ui")[-1]["payload"] == {"text": "Noon, and sunny in Kyoto."}


def test_pending_from_an_ordinary_tool_can_target_another_provider_and_surface_failure(store):
    class Research:
        schema = {"type": "function", "function": {"name": "research"}}

        def execute(self, topic):
            return Pending("research:v1", {"topic": topic})

    def doomed(context, state, mail):
        raise RuntimeError("provider unavailable")

    complete = scripted({"role": "assistant", "content": None, "tool_calls": [{
        "id": "r1", "type": "function", "function": {
            "name": "research", "arguments": '{"topic": "storage"}'}}]},
        {"role": "assistant", "content": "Research failed."})
    worker = dispatcher(store, ChatAgent(complete, [Research()]))
    provider = Dispatcher(store, max_attempts=1).register(Executable("research:v1", doomed))
    worker.create("chat", "chat:v1", {})
    user(store, "chat", "Research storage.", 1)
    assert worker.run_once().committed
    assert not provider.run_once().committed
    store.clock = lambda: 102.0
    assert provider.run_once() is None
    assert store.inspect("chat:r1")["status"] == "failed"
    assert notify_failures(store, "chat") == 1
    assert worker.run_once().committed
    assert complete.seen[1][-1] == {
        "role": "tool", "tool_call_id": "r1", "name": "research",
        "content": "work chat:r1 (research) failed"}
    assert store.inspect("chat")["state"]["panel"] == {}


def test_a_result_from_the_wrong_provider_cannot_fill_a_deferred_slot(store):
    complete = scripted({"role": "assistant", "content": None, "tool_calls": [call("Kyoto")]},
                        {"role": "assistant", "content": "Done."})
    worker = dispatcher(store, ChatAgent(complete, [Lifted(Weather())]))
    runner = Dispatcher(store).register(Executable("tool-runner:v1", ToolRunner([Weather()]).resume))
    worker.create("chat", "chat:v1", {})
    user(store, "chat", "Kyoto?", 1)
    assert worker.run_once().committed
    request_id, = store.inspect("chat")["state"]["panel"]
    store.append("chat", {"event_id": "wrong-result", "kind": "result", "source": "other",
                          "request_id": request_id, "payload": {"text": "wrong"}})
    result = worker.run_once()
    assert not result.committed and "unexpected mail 'wrong-result'" in str(result.error)
    assert request_id in store.inspect("chat")["state"]["panel"]
    assert len(complete.seen) == 1


def test_the_lifted_tool_demo_runs_through_chat_agent_without_a_model(tmp_path):
    script = Path(__file__).resolve().parent.parent / "examples" / "tool_loop.py"
    result = subprocess.run([sys.executable, str(script), str(tmp_path), "--chat-agent"],
                            capture_output=True, text=True, check=True)
    assert "slots=1 held=1" in result.stdout
    assert "Sunny in Kyoto. And yes, I will keep it brief." in result.stdout


def test_a_fresh_process_resumes_the_runner_and_caller_from_the_store(store):
    complete = scripted({"role": "assistant", "content": None, "tool_calls": [call("Kyoto")]})
    worker = dispatcher(store, ChatAgent(complete, [Lifted(Weather())], output="ui"))
    Dispatcher(store).register(Executable("tool-runner:v1", ToolRunner([Weather()]).resume))
    worker.create("chat", "chat:v1", {})
    user(store, "chat", "Kyoto?", 1)
    assert worker.run_once().committed
    user(store, "chat", "Keep it brief.", 2)
    assert worker.run_once().committed

    resumed = subprocess.run([sys.executable, "-c", '''
import sys
from entourage.deferred_tools import Lifted, ToolRunner
from entourage.executables import Dispatcher, Executable
from entourage.sessions import LocalSessions
from entourage.turn import ChatAgent
from examples.tool_loop import Weather, scripted

store = LocalSessions(sys.argv[1], clock=lambda: 100.0)
runner = Dispatcher(store).register(Executable("tool-runner:v1", ToolRunner([Weather()]).resume))
complete = scripted({"role": "assistant", "content": "Sunny in Kyoto."})
caller = Dispatcher(store).register(
    Executable("chat:v1", ChatAgent(complete, [Lifted(Weather())], output="ui").resume))
assert runner.run_once().committed
assert caller.run_once().committed
assert [m["role"] for m in complete.seen[0]] == ["user", "assistant", "tool", "user"]
assert complete.seen[0][-2]["content"] == "Kyoto: sunny"
assert complete.seen[0][-1]["content"] == "Keep it brief."
''', str(store.path)], capture_output=True, text=True)
    assert resumed.returncode == 0, resumed.stderr
    assert store.inspect("chat")["state"]["panel"] == {}
    assert inputs(store, "ui")[-1]["payload"] == {"text": "Sunny in Kyoto."}


@pytest.mark.parametrize("status", ["failed", "cancelled"])
def test_provider_terminal_status_closes_a_wait_with_a_visible_outcome(store, status):
    def provider(context, state, mail):
        context.reply(mail[0], {"status": status, "text": "cannot proceed"})
        return context.propose(state, incorporated=[e["event_id"] for e in mail], complete=True)

    complete = scripted({"role": "assistant", "content": None, "tool_calls": [call("Kyoto")]},
                        {"role": "assistant", "content": "Could not check the weather."})
    worker = dispatcher(store, ChatAgent(complete, [Lifted(Weather(), runner="provider:v1")]))
    provider_worker = Dispatcher(store).register(Executable("provider:v1", provider))
    worker.create("chat", "chat:v1", {})
    user(store, "chat", "Kyoto?", 1)
    assert worker.run_once().committed
    assert provider_worker.run_once().committed
    assert worker.run_once().committed
    assert complete.seen[1][-1] == {
        "role": "tool", "tool_call_id": "c1", "name": "weather",
        "content": f"work chat:c1 (weather) {status}: cannot proceed"}
    assert store.inspect("chat")["state"]["panel"] == {}
