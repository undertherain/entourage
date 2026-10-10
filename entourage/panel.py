"""The panel: an agent's registry of the work it has called.

`Exchanges` grown with what the work protocol needs (docs/work-protocol.md):
each entry remembers how the caller attached (`wait` holds a tool call open
until the result, `connected` lets the caller go on and takes the result as
mail), the work's status, the last question it asked, and a wait budget. The
caller-side controls (steer, cancel, answer) are mail on the same exchange.

Cancel is a fence on this side: the entry is marked and kept, so a late result
is recognised and discarded instead of surfacing as unknown mail. Whether the
provider stops is its own business (cancel mail first; forced shutdown is a
runtime feature not built yet).
"""

from .exchanges import Exchanges

STATUSES = ("working", "input_required", "completed", "failed", "cancelled")
"""MCP's task statuses; more may be added as consumers need them."""


class Panel(Exchanges):
    def __init__(self, state, key="panel"):
        super().__init__(state, key)

    def open(self, context, definition, child_state, payload, *, key, label, mode,
             tool_call_id=None, until=None, interruptible=True):
        """Spawn a provider and send it the request in one checkpoint.

        `mode` is `wait` or `connected`; `until` is the absolute time a wait
        is promoted to `connected` if nothing has arrived. Returns the entry.
        """
        if mode not in ("wait", "connected"):
            raise ValueError(f"mode must be wait or connected, not {mode!r}")
        child = context.spawn(definition, child_state, key=key)
        request_id = context.request(child, payload, key=key)
        self.table[request_id] = {
            "to": child, "label": label, "mode": mode, "status": "working",
            "tool_call_id": tool_call_id, "until": until,
            "interruptible": interruptible, "question": None, "progress": None,
        }
        return request_id

    def ingest(self, mail):
        replies, others = super().ingest(mail)
        for reply in replies:
            entry = reply.entry
            if reply.kind == "ask":
                entry["status"] = "input_required"
                entry["question"] = (reply.payload or {}).get("text")
            elif reply.kind == "progress":
                entry["progress"] = (reply.payload or {}).get("text")
            else:
                status = (reply.payload or {}).get("status", "completed")
                if entry["status"] != "cancelled":
                    entry["status"] = status
        return replies, others

    def find(self, work):
        """Look an entry up by the provider's session ID, the handle shown to the model."""
        for request_id, entry in self.table.items():
            if entry["to"] == work:
                return request_id, entry
        return None, None

    @property
    def waiting(self):
        return {request_id: entry for request_id, entry in self.table.items()
                if entry["mode"] == "wait"}

    def due(self, now):
        """Waits whose budget has passed."""
        return [request_id for request_id, entry in self.waiting.items()
                if entry["until"] is not None and entry["until"] <= now]

    def next_deadline(self):
        budgets = [entry["until"] for entry in self.waiting.values()
                   if entry["until"] is not None]
        return min(budgets) if budgets else None

    def promote(self, request_id):
        """Turn a wait into a connected entry; returns it."""
        entry = self.table[request_id]
        entry["mode"] = "connected"
        return entry

    def steer(self, context, request_id, text, *, key):
        entry = self.table[request_id]
        return context.send(entry["to"], {"text": text}, key=key, kind="steer",
                            request_id=request_id)

    def answer(self, context, request_id, text, *, key):
        entry = self.table[request_id]
        entry["status"], entry["question"] = "working", None
        return context.send(entry["to"], {"text": text}, key=key, kind="answer",
                            request_id=request_id)

    def cancel(self, context, request_id, *, key):
        """Send cancel and fence the entry; its result, if any, is discarded on arrival."""
        entry = self.table[request_id]
        entry["status"], entry["mode"] = "cancelled", "connected"
        return context.send(entry["to"], {}, key=key, kind="cancel", request_id=request_id)

    def fail(self, work):
        """Close every entry to a session the runner reported failed; returns them."""
        failed = []
        for request_id in [r for r, e in self.table.items() if e["to"] == work]:
            entry = self.table.pop(request_id)
            if entry["status"] != "cancelled":
                entry["status"] = "failed"
                failed.append((request_id, entry))
        return failed
