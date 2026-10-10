"""The bare tool loop example: slots for lifted tools, held user text, restart while parked."""

import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "examples"))

from tool_loop import Lifted, ToolLoop, ToolRunner, Weather, answers, demo, scripted, tool_call  # noqa: E402

from entourage.executables import Dispatcher, Executable  # noqa: E402
from entourage.sessions import LocalSessions  # noqa: E402


@pytest.fixture
def store(tmp_path):
    return LocalSessions(tmp_path / "sessions.db", clock=lambda: 100.0)


def test_demo_holds_the_user_until_the_lifted_tool_replies(store):
    lines = []
    complete, _ = demo(store, say=lines.append)
    assert [m["role"] for m in complete.seen[1]] == ["user", "assistant", "tool", "user"]
    assert complete.seen[1][2]["tool_call_id"] == "c1"
    assert answers(store) == ["Sunny in Kyoto. And yes, I will keep it brief."]
    assert "slots=1 held=1" in lines[2]
    chat = store.inspect("chat")["state"]
    assert chat["slots"] == {} and chat["held"] == []


def test_inline_tools_repeat_at_once_and_a_restart_resumes_a_parked_slot(store):
    complete = scripted(
        {"role": "assistant", "content": None, "tool_calls": [tool_call("c1", "clock")]},
        {"role": "assistant", "content": None, "tool_calls": [tool_call("c2", "weather", city="Oslo")]},
        {"role": "assistant", "content": "Noon, and sunny in Oslo."},
    )

    class Clock:
        schema = {"type": "function", "function": {"name": "clock", "parameters": {"type": "object"}}}

        def execute(self):
            return {"hour": 12}

    loop = ToolLoop(complete, [Clock(), Lifted(Weather())], output="ui")
    runner_code = Executable("tool-runner:v1", ToolRunner([Weather()]).resume)
    Dispatcher(store).register(runner_code)      # binds the definition a lifted tool spawns
    worker = Dispatcher(store).register(Executable("loop:v1", loop.resume))
    worker.register(Executable("ui:v1", lambda ctx, state, mail: ctx.propose(
        state, incorporated=[e["event_id"] for e in mail])))
    worker.create("ui", "ui:v1", {})
    worker.create("chat", "loop:v1", {})
    worker.run_until_idle()
    store.append("chat", {"event_id": "u1", "kind": "user", "payload": {"text": "Time and weather?"}})

    assert worker.run_once().committed           # clock inline, repeat at once
    assert store.inspect("chat")["deadline"] == 0.0
    assert worker.run_once().committed           # weather lifted, slot open, parked
    chat = store.inspect("chat")
    assert chat["deadline"] is None and len(chat["state"]["slots"]) == 1
    assert chat["state"]["messages"][-1]["role"] == "assistant"
    assert worker.run_once() is None

    # The worker is gone; fresh ones pick the runner and then the loop up from the store.
    runner = Dispatcher(store).register(runner_code)
    assert runner.run_once().committed
    assert store.inspect("chat:c2")["status"] == "complete"
    again = Dispatcher(store).register(Executable("loop:v1", loop.resume))
    assert again.run_once().committed
    assert complete.seen[2][-1] == {"role": "tool", "tool_call_id": "c2", "content": "Oslo: sunny"}
    assert store.inspect("chat")["state"]["slots"] == {}
    assert answers(store) == ["Noon, and sunny in Oslo."]


def test_the_script_runs_without_a_model(tmp_path):
    script = Path(__file__).resolve().parent.parent / "examples" / "tool_loop.py"
    output = subprocess.run([sys.executable, str(script), str(tmp_path)],
                            check=True, capture_output=True, text=True).stdout
    assert "ui received" in output and "Sunny in Kyoto" in output
