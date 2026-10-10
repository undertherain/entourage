"""One LLM turn with tools, as a resume handler for the session dispatcher.

This is the agent loop `A, tool, A` without a graph. Each activation ingests
mail, calls the model once, runs any requested tools inline and checkpoints.
Tool results are committed before the next model call, so a user message that
arrives while tools run is seen by the model on the next wake. Messages live in
session state; the final answer is mail to an output address.

Pipes add delegation (docs/work-protocol.md, case A): a pipe is a tool that
spawns a provider session and sends it the task. The model picks per call
whether to wait for the result or connect: under wait the tool call stays open
and the session parks; under connect the tool returns at once and the result
arrives later as a message. A wait is promoted to connected when its budget
passes, when the work asks a question, or when a message for the model arrives
and the pipe is interruptible. The same agent can be the provider: a `request`
starts a job whose answer goes back to `reply_to`, and `steer`, `answer` and
`cancel` mail are honoured. The job's status lives under `state["work"]`.
"""

import json
import logging
import time
import traceback

from .panel import Panel

log = logging.getLogger(__name__)

NOW = 0.0
"""A deadline already in the past: the session is claimable again at once."""


def litellm_complete(model, base_url=None, **params):
    """Default model binding: `(messages, tools) -> message dict` over litellm."""
    def complete(messages, tools):
        import warnings
        from litellm import completion

        kwargs = {"model": model, "messages": messages, **params}
        if tools:
            kwargs["tools"] = tools
        if base_url:
            kwargs["base_url"] = base_url
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning, module="pydantic")
            response = completion(**kwargs)
        return message_dict(response.choices[0].message)
    return complete


def message_dict(message):
    """Reduce a provider response message to the JSON shape stored in state."""
    result = {"role": getattr(message, "role", None) or "assistant",
              "content": getattr(message, "content", None)}
    tool_calls = getattr(message, "tool_calls", None)
    if tool_calls:
        result["tool_calls"] = [_tool_call_dict(call) for call in tool_calls]
    return result


def _tool_call_dict(call):
    if isinstance(call, dict):
        return call
    if hasattr(call, "model_dump"):
        return call.model_dump()
    return {"id": call.id, "type": "function",
            "function": {"name": call.function.name, "arguments": call.function.arguments}}


class Pipe:
    """A delegation the agent may open, shown to the model as a tool.

    `definition` is the provider's registered definition (a `ChatAgent` or any
    handler that replies `result` with `payload.text`). `modes` lists what the
    model may pick: `wait`, `connected`, or both (then the tool takes a
    `background` flag). `budget` bounds a wait in seconds before promotion;
    `interruptible` lets a message for the model promote it. `grants` names the
    panel tools the provider honours (a `ChatAgent` provider honours all three).
    """

    def __init__(self, name, definition, *, description=None, modes=("wait", "connected"),
                 budget=None, interruptible=True, grants=("steer", "cancel", "answer")):
        unknown = set(modes) - {"wait", "connected"}
        if not modes or unknown:
            raise ValueError(f"modes must be wait and/or connected, not {modes!r}")
        self.name = name
        self.definition = definition
        self.description = description or f"Delegate a task to {definition}."
        self.modes = tuple(modes)
        self.budget = budget
        self.interruptible = interruptible
        self.grants = tuple(grants)

    @property
    def schema(self):
        properties = {"task": {"type": "string", "description": "What the worker should do."}}
        if len(self.modes) == 2:
            properties["background"] = {"type": "boolean", "description":
                "True to go on and take the result as a message; false to wait for it."}
        return {"type": "function", "function": {
            "name": self.name, "description": self.description,
            "parameters": {"type": "object", "properties": properties, "required": ["task"]}}}


PANEL_TOOLS = {
    "steer": {"type": "function", "function": {
        "name": "steer", "description": "Send an instruction to running work.",
        "parameters": {"type": "object", "properties": {
            "work": {"type": "string"}, "text": {"type": "string"}},
            "required": ["work", "text"]}}},
    "cancel": {"type": "function", "function": {
        "name": "cancel", "description": "Cancel running work; a late result is discarded.",
        "parameters": {"type": "object", "properties": {"work": {"type": "string"}},
                       "required": ["work"]}}},
    "answer": {"type": "function", "function": {
        "name": "answer", "description": "Answer a question running work has asked.",
        "parameters": {"type": "object", "properties": {
            "work": {"type": "string"}, "text": {"type": "string"}},
            "required": ["work", "text"]}}},
}

ASK_TOOL = {"type": "function", "function": {
    "name": "ask_caller", "description":
        "Ask whoever gave you this task a question; their answer arrives as a message.",
    "parameters": {"type": "object", "properties": {"text": {"type": "string"}},
                   "required": ["text"]}}}


class ChatAgent:
    """Resume handler with messages in state and one model call per activation.

    `complete(messages, tool_schemas)` returns an assistant message dict (see
    `litellm_complete`). Tools expose `schema` and `execute(**arguments)`; a tool
    failure is returned to the model as the tool result. `system_prompt` is a
    string or `(context, state) -> str`, inserted once at the first turn.
    `output` is the address answers are sent to; None keeps them in state only.
    `pipes` are `Pipe`s the model may delegate through; `clock` is the time
    source for wait budgets (the store's clock in tests).

    Mail: `kind: user` or `message` with `payload.text` is a user message, the
    timer's `kind: system` is acknowledged, a `request` starts a job for its
    sender, `steer` and `answer` are messages from the caller, `cancel` ends the
    job, and replies on the panel's exchanges are folded in. Anything else fails
    the activation so unknown mail is never dropped silently. Queued mail is
    coalesced into one turn (`has_more` parks first). Register the bound method:
    `Executable("chat:v1", ChatAgent(...).resume)`.
    """

    def __init__(self, complete, tools=(), *, system_prompt=None, output=None, pipes=(),
                 clock=time.time):
        self.complete = complete
        self.tools = {tool.schema["function"]["name"]: tool for tool in tools}
        self.pipes = {pipe.name: pipe for pipe in pipes}
        self.schemas = [tool.schema for tool in tools] + [pipe.schema for pipe in pipes]
        granted = {grant for pipe in pipes for grant in pipe.grants}
        self.schemas += [PANEL_TOOLS[name] for name in PANEL_TOOLS if name in granted]
        self.system_prompt = system_prompt
        self.output = output
        self.clock = clock

    # -- ingestion -----------------------------------------------------------

    def resume(self, context, state, mail):
        messages = state.setdefault("messages", [])
        panel = Panel(state)
        incorporated = [event["event_id"] for event in mail]
        replies, others = panel.ingest(mail)
        for reply in replies:
            self._fold(state, panel, reply)
        for event in others:
            kind = event.get("kind")
            if kind in ("user", "message", "steer", "answer"):
                self._deliver(state, panel, {"role": "user", "content": event["payload"]["text"]})
            elif kind == "request":
                state["work"] = {"status": "working", "request": {
                    "request_id": event["request_id"], "reply_to": event["reply_to"]}}
                self._deliver(state, panel, {"role": "user", "content": event["payload"]["text"]})
            elif kind == "cancel" and self._job(state) is not None:
                return self._finish(context, state, incorporated, "", status="cancelled")
            elif kind == "system" and event.get("source") == "timer":
                pass
            elif kind == "system" and event.get("payload", {}).get("failed"):
                for request_id, entry in panel.fail(event["payload"]["failed"]):
                    self._settle(state, panel, request_id, entry,
                                 f"work {entry['to']} ({entry['label']}) failed")
            else:
                raise ValueError(f"unexpected mail {event.get('event_id')!r} of kind {kind!r}")
        for request_id in panel.due(self.clock()):
            entry = panel.promote(request_id)
            self._defer(state, entry, f"still running as work {entry['to']} ({entry['label']}); "
                                      "the result arrives as a message")
        self._flush(state, panel)
        if context.has_more:
            # Persist this batch and let the rest of the queue join the same turn.
            return context.propose(state, incorporated=incorporated)
        if panel.waiting:
            return context.propose(state, incorporated=incorporated,
                                   deadline=panel.next_deadline())
        if not messages or messages[-1]["role"] not in ("user", "tool"):
            return context.propose(state, incorporated=incorporated)
        job = self._job(state)
        if job and job["status"] == "input_required" and messages[-1]["role"] == "tool":
            return context.propose(state, incorporated=incorporated)  # the question is out
        return self._turn(context, state, panel, incorporated)

    def _fold(self, state, panel, reply):
        """Apply one reply from a provider to its panel entry and the conversation."""
        entry = reply.entry
        text = (reply.payload or {}).get("text") or ""
        if reply.kind == "progress":
            return
        if reply.kind == "ask":
            self._settle(state, panel, reply.request_id, entry,
                         f"work {entry['to']} ({entry['label']}) asks: {text} "
                         f"(reply with answer, work={entry['to']!r})")
            return
        if entry["status"] == "cancelled":
            return  # fenced: the late result is discarded
        self._settle(state, panel, reply.request_id, entry,
                     f"work {entry['to']} ({entry['label']}) {entry['status']}: {text}"
                     if entry["mode"] == "connected" else text)

    def _settle(self, state, panel, request_id, entry, content):
        """Write the outcome where the mode says: as the tool result or as a message."""
        if entry["mode"] == "wait":
            if request_id in panel.table and entry["status"] != "completed":
                panel.promote(request_id)  # a question or failure ends the wait early
            self._defer(state, entry, content)
        else:
            self._deliver(state, panel, {"role": "user", "content": f"[{content}]"})

    def _defer(self, state, entry, content):
        state.setdefault("deferred", []).append({
            "role": "tool", "tool_call_id": entry["tool_call_id"], "name": entry["label"],
            "content": content})

    def _deliver(self, state, panel, message):
        """Queue a message for the model; it promotes interruptible waits."""
        for request_id, entry in panel.waiting.items():
            if entry["interruptible"]:
                panel.promote(request_id)
                self._defer(state, entry, f"still running as work {entry['to']} "
                                          f"({entry['label']}); the result arrives as a message")
        state.setdefault("buffered", []).append(message)

    def _flush(self, state, panel):
        """Once no tool call is held open, append deferred results, then buffered messages."""
        if panel.waiting:
            return
        messages = state["messages"]
        messages.extend(state.pop("deferred", []))
        messages.extend(state.pop("buffered", []))

    # -- the model call and its tools ---------------------------------------

    def _turn(self, context, state, panel, incorporated):
        messages = state["messages"]
        self._ensure_system_prompt(context, state)
        schemas = self.schemas + ([ASK_TOOL] if self._job(state) else [])
        reply = self.complete(messages, schemas)
        if not isinstance(reply, dict) or reply.get("role") != "assistant":
            raise TypeError("complete must return an assistant message dict")
        messages.append(reply)
        if reply.get("tool_calls"):
            asked = False
            for call in reply["tool_calls"]:
                asked |= self._run_call(context, state, panel, call)
            if panel.waiting:
                deadline = panel.next_deadline()
            else:
                deadline = None if asked else NOW
            # Checkpoint the tool results; the next activation calls the model again.
            return context.propose(state, incorporated=incorporated, deadline=deadline)
        content = reply.get("content") or ""
        if self._job(state) is not None:
            return self._finish(context, state, incorporated, content)
        if self.output is not None:
            context.send(self.output, {"text": content}, key=f"answer:{len(messages)}")
        return context.propose(state, incorporated=incorporated)

    def _run_call(self, context, state, panel, call):
        """Run one tool call; returns True when it asked the caller a question."""
        name = call["function"]["name"]
        arguments = json.loads(call["function"]["arguments"] or "{}")
        if name in self.pipes:
            self._delegate(context, state, panel, call, self.pipes[name], arguments)
            return False
        if name in PANEL_TOOLS and name in {g for p in self.pipes.values() for g in p.grants}:
            content = self._control(context, state, panel, call, name, arguments)
        elif name == "ask_caller" and self._job(state) is not None:
            job = self._job(state)
            context.send(job["request"]["reply_to"], {"text": arguments["text"]},
                         key=call["id"], kind="ask", request_id=job["request"]["request_id"])
            job["status"] = "input_required"
            content = "asked; the answer arrives as a message"
        else:
            content = self._run_tool(name, arguments)
        state["messages"].append({"role": "tool", "tool_call_id": call["id"], "name": name,
                                  "content": content})
        return name == "ask_caller"

    def _delegate(self, context, state, panel, call, pipe, arguments):
        background = arguments.get("background", pipe.modes[0] == "connected")
        mode = "connected" if background else "wait"
        if mode not in pipe.modes:
            mode = pipe.modes[0]
        until = self.clock() + pipe.budget if mode == "wait" and pipe.budget else None
        request_id = panel.open(context, pipe.definition, {"messages": []},
                                {"text": arguments["task"]}, key=call["id"], label=pipe.name,
                                mode=mode, tool_call_id=call["id"], until=until,
                                interruptible=pipe.interruptible)
        if mode == "connected":
            work = panel.table[request_id]["to"]
            state["messages"].append({
                "role": "tool", "tool_call_id": call["id"], "name": pipe.name,
                "content": f"started as work {work} ({pipe.name}); "
                           "the result arrives as a message"})

    def _control(self, context, state, panel, call, name, arguments):
        request_id, entry = panel.find(arguments.get("work"))
        if entry is None:
            return f"Tool error: no such work {arguments.get('work')!r}"
        if name == "steer":
            panel.steer(context, request_id, arguments["text"], key=call["id"])
            return f"steered work {entry['to']}"
        if name == "answer":
            if entry["status"] != "input_required":
                return f"Tool error: work {entry['to']} asked nothing"
            panel.answer(context, request_id, arguments["text"], key=call["id"])
            return f"answered work {entry['to']}"
        if entry["status"] == "cancelled":
            return f"work {entry['to']} is already cancelled"
        was_waiting = entry["mode"] == "wait"
        panel.cancel(context, request_id, key=call["id"])
        if was_waiting:
            self._defer(state, entry, f"work {entry['to']} ({entry['label']}) cancelled")
        return f"cancelled work {entry['to']}"

    def _finish(self, context, state, incorporated, content, *, status="completed"):
        """Reply to the caller and complete: one request per provider session."""
        job = state["work"]
        job["status"] = status
        context.reply(job["request"], {"text": content, "status": status})
        return context.propose(state, incorporated=incorporated, complete=True)

    @staticmethod
    def _job(state):
        job = state.get("work")
        return job if job and job["status"] in ("working", "input_required") else None

    def _ensure_system_prompt(self, context, state):
        messages = state["messages"]
        if self.system_prompt is None or (messages and messages[0]["role"] == "system"):
            return
        prompt = self.system_prompt
        if callable(prompt):
            prompt = prompt(context, state)
        messages.insert(0, {"role": "system", "content": prompt})

    def _run_tool(self, name, arguments):
        log.info("Calling tool %s", name)
        try:
            tool = self.tools[name]
        except KeyError:
            return f"Tool error: unknown tool {name!r}"
        try:
            result = tool.execute(**arguments)
            return result if isinstance(result, str) else json.dumps(result)
        except Exception as exc:  # noqa: BLE001 - the model must see the failure
            log.debug("Tool %s failed:\n%s", name, traceback.format_exc())
            return f"Tool error ({type(exc).__name__}): {exc}"
