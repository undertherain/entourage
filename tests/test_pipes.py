"""Case A of the work protocol: one ChatAgent delegating to another through a pipe.

Wait holds the tool call open and parks; connected returns at once and takes the
result as a message; promotion turns the first into the second on a budget, a
question, an interruption or a cancel. The provider side honours request,
steer, answer and cancel and keeps its status under `state["work"]`.
"""

import json
import sqlite3

import pytest

from entourage.executables import Dispatcher, Executable
from entourage.runner import notify_failures
from entourage.sessions import LocalSessions
from entourage.turn import ChatAgent, Pipe


def scripted(*replies):
    """A `complete` that returns the scripted replies in order and records what it saw."""
    seen, tools_seen = [], []

    def complete(messages, tools):
        seen.append([dict(m) for m in messages])
        tools_seen.append([t["function"]["name"] for t in tools])
        return dict(replies[len(seen) - 1])
    complete.seen, complete.tools_seen = seen, tools_seen
    return complete


def say(text):
    return {"role": "assistant", "content": text}


def calls(*items):
    return {"role": "assistant", "content": None, "tool_calls": [
        {"id": call_id, "type": "function",
         "function": {"name": name, "arguments": json.dumps(arguments)}}
        for call_id, name, arguments in items]}


def inputs(store, destination):
    with sqlite3.connect(store.path) as db:
        return [json.loads(row[0]) for row in db.execute(
            "SELECT event FROM wake_inputs WHERE session_id = ? ORDER BY seq", (destination,))]


def user(store, session, text, key):
    store.append(session, {"event_id": f"user:{key}", "kind": "user", "payload": {"text": text}})


@pytest.fixture
def clock():
    return [100.0]


@pytest.fixture
def store(tmp_path, clock):
    return LocalSessions(tmp_path / "sessions.db", clock=lambda: clock[0])


def ui(store):
    """A parked `ui` session that collects the parent's answers; nobody runs it."""
    worker = Dispatcher(store).register(Executable("ui:v1", lambda ctx, state, mail: ctx.propose(
        state, incorporated=[e["event_id"] for e in mail])))
    worker.create("ui", "ui:v1", {})
    assert worker.run_once().session_id == "ui"


def workers(store, parent, child_resume, **child_options):
    """One dispatcher per side, so a test controls who runs when."""
    ui(store)
    parent_worker = Dispatcher(store).register(Executable("chat:v1", parent.resume))
    child_worker = Dispatcher(store, **child_options).register(
        Executable("worker:v1", child_resume))
    parent_worker.create("chat", "chat:v1", {})
    return parent_worker, child_worker


def pipe(**options):
    return Pipe("research", "worker:v1", **options)


def test_wait_holds_the_tool_call_open_and_a_fresh_worker_folds_the_result(store, clock):
    parent = ChatAgent(scripted(calls(("c1", "research", {"task": "find X"})),
                                say("X is 42.")),
                       system_prompt="Delegate.", pipes=[pipe()], output="ui",
                       clock=lambda: clock[0])
    child = ChatAgent(scripted(say("42")))
    parent_worker, child_worker = workers(store, parent, child.resume)
    user(store, "chat", "What is X?", 1)

    assert parent_worker.run_once().committed
    chat = store.inspect("chat")
    assert chat["status"] == "waiting" and chat["deadline"] is None
    assert [m["role"] for m in chat["state"]["messages"]] == ["system", "user", "assistant"]
    (entry,) = chat["state"]["panel"].values()
    assert entry["to"] == "chat:c1" and entry["mode"] == "wait" and entry["status"] == "working"
    assert store.inspect("chat:c1")["status"] == "ready"  # spawned with the request as mail
    assert parent_worker.run_once() is None  # nothing to do until the result arrives

    # The worker that opened the pipe is gone; new ones continue from the store.
    again_child = Dispatcher(store).register(Executable("worker:v1", child.resume))
    assert again_child.run_once().committed
    worker_state = store.inspect("chat:c1")
    assert worker_state["status"] == "complete"
    assert worker_state["state"]["work"]["status"] == "completed"
    assert "ask_caller" in child.complete.tools_seen[0]

    again_parent = Dispatcher(store).register(Executable("chat:v1", parent.resume))
    assert again_parent.run_once().committed
    assert parent.complete.seen[1][-1] == {"role": "tool", "tool_call_id": "c1",
                                           "name": "research", "content": "42"}
    assert [m["role"] for m in parent.complete.seen[1]] == ["system", "user", "assistant", "tool"]
    assert store.inspect("chat")["state"]["panel"] == {}
    assert inputs(store, "ui")[-1]["payload"] == {"text": "X is 42."}


def test_connected_returns_at_once_and_steer_reaches_the_worker(store, clock):
    parent = ChatAgent(scripted(calls(("c1", "research", {"task": "find X", "background": True})),
                                calls(("c2", "steer", {"work": "chat:c1", "text": "be brief"})),
                                say("Working on it."), say("X is 42.")),
                       pipes=[pipe()], output="ui", clock=lambda: clock[0])
    child = ChatAgent(scripted(say("42")))
    parent_worker, child_worker = workers(store, parent, child.resume)
    user(store, "chat", "What is X?", 1)

    assert parent_worker.run_once().committed
    messages = store.inspect("chat")["state"]["messages"]
    assert messages[-1]["role"] == "tool" and messages[-1]["content"].startswith(
        "started as work chat:c1 (research)")
    assert parent_worker.run_once().committed  # steer
    assert parent_worker.run_once().committed  # "Working on it."
    assert parent_worker.run_once() is None
    assert "steer" in parent.complete.tools_seen[0]

    assert child_worker.run_once().committed
    assert [m["role"] for m in child.complete.seen[0]] == ["user", "user"]
    assert child.complete.seen[0][1]["content"] == "be brief"

    assert parent_worker.run_once().committed
    assert parent.complete.seen[3][-1] == {
        "role": "user", "content": "[work chat:c1 (research) completed: 42]"}
    assert [e["payload"]["text"] for e in inputs(store, "ui")] == ["Working on it.", "X is 42."]


def test_a_user_message_promotes_an_interruptible_wait(store, clock):
    parent = ChatAgent(scripted(calls(("c1", "research", {"task": "find X"})),
                                say("Still looking; noted Y."), say("X is 42.")),
                       pipes=[pipe()], output="ui", clock=lambda: clock[0])
    child = ChatAgent(scripted(say("42")))
    parent_worker, child_worker = workers(store, parent, child.resume)
    user(store, "chat", "What is X?", 1)
    assert parent_worker.run_once().committed

    user(store, "chat", "Also Y.", 2)
    assert parent_worker.run_once().committed
    tail = parent.complete.seen[1][-2:]
    assert tail[0]["role"] == "tool" and tail[0]["tool_call_id"] == "c1"
    assert tail[0]["content"].startswith("still running as work chat:c1")
    assert tail[1] == {"role": "user", "content": "Also Y."}
    (entry,) = store.inspect("chat")["state"]["panel"].values()
    assert entry["mode"] == "connected"

    assert child_worker.run_once().committed
    assert parent_worker.run_once().committed
    assert parent.complete.seen[2][-1]["content"] == "[work chat:c1 (research) completed: 42]"


def test_a_strict_wait_buffers_the_user_until_the_result(store, clock):
    parent = ChatAgent(scripted(calls(("c1", "research", {"task": "find X"})), say("X is 42; Y noted.")),
                       pipes=[pipe(interruptible=False)], output="ui", clock=lambda: clock[0])
    child = ChatAgent(scripted(say("42")))
    parent_worker, child_worker = workers(store, parent, child.resume)
    user(store, "chat", "What is X?", 1)
    assert parent_worker.run_once().committed
    user(store, "chat", "Also Y.", 2)
    assert parent_worker.run_once().committed  # buffered, no model call
    assert len(parent.complete.seen) == 1
    assert store.inspect("chat")["state"]["buffered"] == [{"role": "user", "content": "Also Y."}]

    assert child_worker.run_once().committed
    assert parent_worker.run_once().committed
    assert [m["role"] for m in parent.complete.seen[1]] == ["user", "assistant", "tool", "user"]
    assert parent.complete.seen[1][-2]["content"] == "42"


def test_a_wait_budget_promotes_on_the_deadline(store, clock):
    parent = ChatAgent(scripted(calls(("c1", "research", {"task": "find X"})),
                                say("Taking a while."), say("X is 42.")),
                       pipes=[pipe(budget=10)], output="ui", clock=lambda: clock[0])
    child = ChatAgent(scripted(say("42")))
    parent_worker, child_worker = workers(store, parent, child.resume)
    user(store, "chat", "What is X?", 1)
    assert parent_worker.run_once().committed
    assert store.inspect("chat")["deadline"] == 110.0
    assert parent_worker.run_once() is None

    clock[0] = 111.0
    assert parent_worker.run_once().committed
    assert parent.complete.seen[1][-1]["content"].startswith("still running as work chat:c1")
    assert store.inspect("chat")["deadline"] is None
    assert child_worker.run_once().committed
    assert parent_worker.run_once().committed
    assert inputs(store, "ui")[-1]["payload"] == {"text": "X is 42."}


def test_a_question_is_the_early_tool_result_and_answer_resumes_the_worker(store, clock):
    parent = ChatAgent(scripted(calls(("c1", "research", {"task": "find X"})),
                                calls(("c2", "answer", {"work": "chat:c1", "text": "the first"})),
                                say("Answered; waiting."), say("X1 is 42.")),
                       pipes=[pipe()], output="ui", clock=lambda: clock[0])
    child = ChatAgent(scripted(calls(("w1", "ask_caller", {"text": "which X?"})), say("42")))
    parent_worker, child_worker = workers(store, parent, child.resume)
    user(store, "chat", "What is X?", 1)
    assert parent_worker.run_once().committed

    assert child_worker.run_once().committed
    worker_state = store.inspect("chat:c1")
    assert worker_state["status"] == "waiting" and worker_state["deadline"] is None
    assert worker_state["state"]["work"]["status"] == "input_required"
    assert child_worker.run_once() is None

    assert parent_worker.run_once().committed
    question = parent.complete.seen[1][-1]
    assert question["role"] == "tool" and question["tool_call_id"] == "c1"
    assert question["content"] == ("work chat:c1 (research) asks: which X? "
                                   "(reply with answer, work='chat:c1')")
    (entry,) = store.inspect("chat")["state"]["panel"].values()
    assert entry["mode"] == "connected" and entry["status"] == "working"
    assert parent_worker.run_once().committed  # "Answered; waiting."

    assert child_worker.run_once().committed
    assert child.complete.seen[1][-1] == {"role": "user", "content": "the first"}
    assert store.inspect("chat:c1")["state"]["work"]["status"] == "completed"
    assert parent_worker.run_once().committed
    assert inputs(store, "ui")[-1]["payload"] == {"text": "X1 is 42."}


def test_cancel_fences_the_entry_and_the_worker_stops(store, clock):
    parent = ChatAgent(scripted(calls(("c1", "research", {"task": "find X", "background": True})),
                                calls(("c2", "cancel", {"work": "chat:c1"})),
                                say("Cancelled.")),
                       pipes=[pipe()], output="ui", clock=lambda: clock[0])
    child = ChatAgent(scripted(say("should never be called")))
    parent_worker, child_worker = workers(store, parent, child.resume)
    user(store, "chat", "What is X?", 1)
    assert parent_worker.run_once().committed
    assert parent_worker.run_once().committed  # cancel sent
    assert parent_worker.run_once().committed  # "Cancelled."
    (entry,) = store.inspect("chat")["state"]["panel"].values()
    assert entry["status"] == "cancelled"

    assert child_worker.run_once().committed  # request and cancel in one batch
    worker_state = store.inspect("chat:c1")
    assert worker_state["status"] == "complete"
    assert worker_state["state"]["work"]["status"] == "cancelled"
    assert child.complete.seen == []

    assert parent_worker.run_once().committed  # the late result: discarded, no model call
    assert store.inspect("chat")["state"]["panel"] == {}
    assert len(parent.complete.seen) == 3


def test_a_failed_worker_ends_the_wait_with_a_failure_result(store, clock):
    parent = ChatAgent(scripted(calls(("c1", "research", {"task": "find X"})),
                                say("The worker failed.")),
                       pipes=[pipe()], output="ui", clock=lambda: clock[0])

    def doomed(ctx, state, mail):
        raise RuntimeError("boom")

    parent_worker, child_worker = workers(store, parent, doomed, max_attempts=1)
    user(store, "chat", "What is X?", 1)
    assert parent_worker.run_once().committed

    assert not child_worker.run_once().committed
    clock[0] += 2
    assert child_worker.run_once() is None
    assert store.inspect("chat:c1")["status"] == "failed"
    assert notify_failures(store, "chat") == 1
    assert parent_worker.run_once().committed
    assert parent.complete.seen[1][-1] == {"role": "tool", "tool_call_id": "c1", "name": "research",
                                           "content": "work chat:c1 (research) failed"}
    assert store.inspect("chat")["state"]["panel"] == {}
    assert inputs(store, "ui")[-1]["payload"] == {"text": "The worker failed."}
