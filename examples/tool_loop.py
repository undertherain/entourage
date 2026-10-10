"""The classic loop on the session core, written bare.

    read the user's message, call the model, run tools if it asks,
    repeat until it answers, then wait for the next message

No `ChatAgent`, no panel: one handler, each line of that loop visible. The
only addition durability forces is the slot: a tool may answer `Pending`
("run this elsewhere") instead of a result. The loop then leaves the tool
call open, parks, and is woken by the result as mail. A user message that
arrives while a slot is open is held in state until the slot fills, so the
model never sees a tool call without its result.

`python examples/tool_loop.py` runs a scripted demo with no model or key.
"""

import json
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path

from entourage.executables import Dispatcher, Executable
from entourage.sessions import LocalSessions

NOW = 0.0
"""A deadline already in the past: the session runs again at once."""


# -- tools: inline, or lifted into a session of their own ------------------

@dataclass(frozen=True)
class Pending:
    """A tool's answer "run this elsewhere": what to spawn and what to send it."""
    definition: str
    payload: dict


class Lifted:
    """Run a tool in its own session, so the loop's turn ends and the tool has its own lease."""

    def __init__(self, tool, runner="tool-runner:v1"):
        self.tool, self.schema, self.runner = tool, tool.schema, runner

    def execute(self, **arguments):
        return Pending(self.runner, {"name": self.schema["function"]["name"],
                                     "arguments": arguments})


class ToolRunner:
    """A session that runs one tool call and replies with the result."""

    def __init__(self, tools):
        self.tools = {tool.schema["function"]["name"]: tool for tool in tools}

    def resume(self, context, state, mail):
        request = next(event for event in mail if event["kind"] == "request")
        context.reply(request, {"text": run_tool(self.tools, request["payload"]["name"],
                                                 request["payload"]["arguments"])})
        return context.propose(state, incorporated=[e["event_id"] for e in mail], complete=True)


def run_tool(tools, name, arguments):
    try:
        result = tools[name].execute(**arguments)
    except KeyError:
        return f"Tool error: unknown tool {name!r}"
    except Exception as exc:  # noqa: BLE001 - the model must see the failure
        return f"Tool error ({type(exc).__name__}): {exc}"
    return result if isinstance(result, (str, Pending)) else json.dumps(result)


# -- the loop ---------------------------------------------------------------

class ToolLoop:
    """`complete(messages, schemas) -> assistant message dict`; answers are mail to `output`.

    Three places to continue from, each a method that ends in a proposal:
    `call_model` when a user message or a tool result is in; `call_tools` when
    the model asked for tools, then exit; `ask_user` when it answered, then
    exit. `resume` only reads the mail and picks the place.
    """

    def __init__(self, complete, tools, *, output):
        self.complete = complete
        self.tools = {tool.schema["function"]["name"]: tool for tool in tools}
        self.schemas = [tool.schema for tool in tools]
        self.output = output

    def resume(self, context, state, mail):
        messages = state.setdefault("messages", [])
        slots = state.setdefault("slots", {})   # request_id -> tool_call_id, results not in yet
        held = state.setdefault("held", [])     # user text that arrived while a slot was open
        done = [event["event_id"] for event in mail]
        for event in mail:
            kind = event["kind"]
            if kind == "user" and slots:
                held.append(event["payload"]["text"])
            elif kind == "user":
                messages.append({"role": "user", "content": event["payload"]["text"]})
            elif kind == "result" and event.get("request_id") in slots:
                messages.append({"role": "tool", "tool_call_id": slots.pop(event["request_id"]),
                                 "content": event["payload"]["text"]})
            elif kind == "system" and event.get("source") == "timer":
                pass
            else:
                raise ValueError(f"unexpected mail {event['event_id']!r} of kind {kind!r}")
        if context.has_more or slots:
            return context.propose(state, incorporated=done)   # more mail, or a tool still runs
        messages.extend({"role": "user", "content": text} for text in held)
        held.clear()
        if messages and messages[-1]["role"] in ("user", "tool"):
            return self.call_model(context, state, done)        # a user message or a tool result
        return context.propose(state, incorporated=done)        # nothing new: keep waiting

    def call_model(self, context, state, done):
        reply = self.complete(state["messages"], self.schemas)
        state["messages"].append(reply)
        if reply.get("tool_calls"):
            return self.call_tools(context, state, done, reply["tool_calls"])
        return self.ask_user(context, state, done, reply.get("content") or "")

    def call_tools(self, context, state, done, calls):
        """Run the tools and exit; a Pending outcome leaves its slot open."""
        for call in calls:
            arguments = json.loads(call["function"]["arguments"] or "{}")
            outcome = run_tool(self.tools, call["function"]["name"], arguments)
            if isinstance(outcome, Pending):
                child = context.spawn(outcome.definition, {}, key=call["id"])
                request_id = context.request(child, outcome.payload, key=call["id"])
                state["slots"][request_id] = call["id"]
            else:
                state["messages"].append({"role": "tool", "tool_call_id": call["id"],
                                          "name": call["function"]["name"], "content": outcome})
        # Every result in: continue at once. A slot open: park until its result is mail.
        return context.propose(state, incorporated=done,
                               deadline=None if state["slots"] else NOW)

    def ask_user(self, context, state, done, answer):
        """Deliver the answer and exit; the next user message wakes the loop."""
        context.send(self.output, {"text": answer}, key=f"answer:{len(state['messages'])}")
        return context.propose(state, incorporated=done)


# -- a scripted demo ---------------------------------------------------------

class Weather:
    schema = {"type": "function", "function": {"name": "weather", "parameters": {
        "type": "object", "properties": {"city": {"type": "string"}}}}}

    def execute(self, city):
        return f"{city}: sunny"


def scripted(*replies):
    seen = []

    def complete(messages, schemas):
        seen.append([dict(m) for m in messages])
        return dict(replies[len(seen) - 1])
    complete.seen = seen
    return complete


def tool_call(call_id, name, **arguments):
    return {"id": call_id, "type": "function",
            "function": {"name": name, "arguments": json.dumps(arguments)}}


def answers(store, session="ui"):
    with sqlite3.connect(store.path) as db:
        return [json.loads(row[0])["payload"]["text"] for row in db.execute(
            "SELECT event FROM wake_inputs WHERE session_id = ? ORDER BY seq", (session,))]


def demo(store, say=print):
    """Two turns: a lifted tool parks the loop, the user types meanwhile, the answer still lands."""
    complete = scripted(
        {"role": "assistant", "content": None, "tool_calls": [tool_call("c1", "weather", city="Kyoto")]},
        {"role": "assistant", "content": "Sunny in Kyoto. And yes, I will keep it brief."},
    )
    loop = ToolLoop(complete, [Lifted(Weather())], output="ui")
    # One dispatcher per definition, so the demo controls who runs when; a
    # deployment would run them all in one worker and let it interleave.
    ui = Dispatcher(store).register(Executable("ui:v1", lambda ctx, state, mail: ctx.propose(
        state, incorporated=[e["event_id"] for e in mail])))
    ui.create("ui", "ui:v1", {})
    ui.run_until_idle()
    worker = Dispatcher(store).register(Executable("loop:v1", loop.resume))
    runner = Dispatcher(store).register(
        Executable("tool-runner:v1", ToolRunner([Weather()]).resume), lease_seconds=600)
    worker.create("chat", "loop:v1", {})
    worker.run_until_idle()

    def status(label):
        chat = store.inspect("chat")
        say(f"{label:<44} chat={chat['status']:<8} slots={len(chat['state'].get('slots', {}))} "
            f"held={len(chat['state'].get('held', []))}")

    store.append("chat", {"event_id": "u1", "kind": "user", "payload": {"text": "Weather in Kyoto?"}})
    status("user asks")
    worker.run_once()                              # the loop: model call, weather is Pending
    status("model called weather: lifted, slot open")
    store.append("chat", {"event_id": "u2", "kind": "user", "payload": {"text": "Keep it brief."}})
    worker.run_once()                              # the loop wakes with the user message: held
    status("user typed while the tool runs: held")
    runner.run_once()                              # the runner: runs the tool, replies, completes
    status("runner replied")
    worker.run_once()                              # the loop: result fills the slot, held text follows
    status("loop resumed: model answered")
    ui.run_until_idle()
    say(f"{'model saw':<44} " + " | ".join(
        m["role"] + (":" + m["content"] if m["role"] != "assistant" else "")
        for m in complete.seen[-1]))
    say(f"{'ui received':<44} {answers(store)}")
    return complete, loop


if __name__ == "__main__":
    base = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")
    db = base / "tool_loop.db"
    if db.exists():
        db.unlink()
    demo(LocalSessions(db))
