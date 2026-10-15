"""Tool outcomes that can release the caller's activation while work runs.

Tools keep the ordinary ``schema`` / ``execute(**arguments)`` interface. An
execution returns a result or ``Pending`` describing a provider to spawn and
request at the caller's checkpoint. ``Lifted`` adapts a plain tool; register
``ToolRunner.resume`` separately to execute it under its own activation lease.
No live tool object or Python stack is part of the saved call.
"""

from dataclasses import dataclass
import json
import logging


log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Pending:
    """Run this request in a new session bound to ``definition``.

    ``payload`` is JSON data. The caller stages child creation, the request and
    its outstanding-call record in one checkpoint. The provider replies with
    ``payload.text`` using ``context.reply``. Returning this value does not
    itself launch work.
    """

    definition: str
    payload: dict


class Lifted:
    """Expose the same tool schema, but execute through a registered ToolRunner.

    The runner owns the actual tool instances. It must finish within its own
    lease; lifting does not add lease renewal, steering or forced cancellation.
    """

    def __init__(self, tool, runner="tool-runner:v1"):
        self.schema = tool.schema
        self.runner = runner

    def execute(self, **arguments):
        return Pending(self.runner, {"name": self.schema["function"]["name"],
                                     "arguments": arguments})


def run_tool(tools, name, arguments):
    """Return text or Pending; lookup, execution and encoding errors become text."""
    log.info("Calling tool %s", name)
    try:
        tool = tools[name]
    except KeyError:
        return f"Tool error: unknown tool {name!r}"
    try:
        result = tool.execute(**arguments)
        return result if isinstance(result, (str, Pending)) else json.dumps(result)
    except Exception as exc:  # noqa: BLE001 - the model must see the failure
        log.debug("Tool %s failed", name, exc_info=True)
        return f"Tool error ({type(exc).__name__}): {exc}"


class ToolRunner:
    """Run one plain tool request, reply and complete the provider session.

    Requests carry ``name`` and ``arguments``. Other application mail is an
    error: this runner does not implement steering, questions or cancellation.
    A bounded input batch is checkpointed before execution when more mail is
    queued, so no delivered input is silently acknowledged.
    """

    def __init__(self, tools):
        self.tools = {tool.schema["function"]["name"]: tool for tool in tools}

    def resume(self, context, state, mail):
        work = state.get("work")
        for event in mail:
            if event["kind"] == "request" and work is None:
                work = state["work"] = {"status": "working", "request": event}
            elif event["kind"] == "system" and event.get("source") == "timer":
                pass
            else:
                raise ValueError(f"unexpected mail {event['event_id']!r} "
                                 f"of kind {event['kind']!r}")
        done = [event["event_id"] for event in mail]
        if context.has_more or work is None:
            return context.propose(state, incorporated=done)
        request = work["request"]
        payload = request["payload"]
        result = run_tool(self.tools, payload["name"], payload["arguments"])
        if isinstance(result, Pending):
            result = "Tool error: a lifted tool must return a final result, not Pending"
        work["status"] = "completed"
        context.reply(request, {"text": result})
        return context.propose(state, incorporated=done, complete=True)
