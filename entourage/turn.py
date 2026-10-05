"""One LLM turn with tools, as a resume handler for the session dispatcher.

This is the agent loop `A, tool, A` without a graph. Each activation ingests
mail, calls the model once, runs any requested tools inline and checkpoints.
Tool results are committed before the next model call, so a user message that
arrives while tools run is seen by the model on the next wake. Messages live in
session state; the final answer is mail to an output address.
"""

import json
import logging
import traceback

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


class ChatAgent:
    """Resume handler with messages in state and one model call per activation.

    `complete(messages, tool_schemas)` returns an assistant message dict (see
    `litellm_complete`). Tools expose `schema` and `execute(**arguments)`; a tool
    failure is returned to the model as the tool result. `system_prompt` is a
    string or `(context, state) -> str`, inserted once at the first turn.
    `output` is the address answers are sent to; None keeps them in state only.

    Mail: `kind: user` or `message` with `payload.text` is a user message, the
    timer's `kind: system` is acknowledged, anything else fails the activation
    so unknown mail is never dropped silently. Queued mail is coalesced into one
    turn (`has_more` parks first). Register the bound method:
    `Executable("chat:v1", ChatAgent(...).resume)`.
    """

    def __init__(self, complete, tools=(), *, system_prompt=None, output=None):
        self.complete = complete
        self.tools = {tool.schema["function"]["name"]: tool for tool in tools}
        self.schemas = [tool.schema for tool in tools]
        self.system_prompt = system_prompt
        self.output = output

    def resume(self, context, state, mail):
        messages = state.setdefault("messages", [])
        incorporated = []
        for event in mail:
            kind = event.get("kind")
            if kind in ("user", "message"):
                messages.append({"role": "user", "content": event["payload"]["text"]})
            elif kind == "system" and event.get("source") == "timer":
                pass
            else:
                raise ValueError(f"unexpected mail {event.get('event_id')!r} of kind {kind!r}")
            incorporated.append(event["event_id"])
        if context.has_more:
            # Persist this batch and let the rest of the queue join the same turn.
            return context.propose(state, incorporated=incorporated)
        if not messages or messages[-1]["role"] not in ("user", "tool"):
            return context.propose(state, incorporated=incorporated)
        self._ensure_system_prompt(context, state)
        reply = self.complete(messages, self.schemas)
        if not isinstance(reply, dict) or reply.get("role") != "assistant":
            raise TypeError("complete must return an assistant message dict")
        messages.append(reply)
        if reply.get("tool_calls"):
            for call in reply["tool_calls"]:
                messages.append(self._run_tool(call))
            # Checkpoint the tool results; the next activation calls the model again.
            return context.propose(state, incorporated=incorporated, deadline=NOW)
        if self.output is not None:
            context.send(self.output, {"text": reply.get("content") or ""},
                         key=f"answer:{len(messages)}")
        return context.propose(state, incorporated=incorporated)

    def _ensure_system_prompt(self, context, state):
        messages = state["messages"]
        if self.system_prompt is None or (messages and messages[0]["role"] == "system"):
            return
        prompt = self.system_prompt
        if callable(prompt):
            prompt = prompt(context, state)
        messages.insert(0, {"role": "system", "content": prompt})

    def _run_tool(self, call):
        function = call["function"]
        name = function["name"]
        log.info("Calling tool %s", name)
        try:
            tool = self.tools[name]
        except KeyError:
            content = f"Tool error: unknown tool {name!r}"
        else:
            try:
                result = tool.execute(**json.loads(function["arguments"] or "{}"))
                content = result if isinstance(result, str) else json.dumps(result)
            except Exception as exc:  # noqa: BLE001 - the model must see the failure
                log.debug("Tool %s failed:\n%s", name, traceback.format_exc())
                content = f"Tool error ({type(exc).__name__}): {exc}"
        return {"role": "tool", "tool_call_id": call["id"], "name": name, "content": content}
