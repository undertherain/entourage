"""Pending request/reply bookkeeping kept in session state.

The runtime never inspects exchanges: `Context.request` returns a stable
request ID and the handler must remember it to recognize the reply. This
helper keeps that table under one state key and splits delivered mail into
the replies it was waiting for and everything else, so a handler can run
`all` or `any` joins and still take steering while parked. A strict join
that must not act on unrelated mail simply stores the "others" in state and
parks again; the backend stays always-interruptible.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Reply:
    request_id: str
    label: str
    source: str
    payload: object
    event: dict


class Exchanges:
    """The pending-exchange table of one session, stored under `state[key]`.

    Construct it on every activation over the restored state; mutations land
    in the state the handler proposes. Replies are matched on `request_id` and
    on the expected sender, so a result from anywhere else is ordinary mail.
    """

    def __init__(self, state, key="exchanges"):
        self.table = state.setdefault(key, {})

    def request(self, context, destination, payload, *, key, label=None):
        """Stage a request and remember it; returns the request ID."""
        request_id = context.request(destination, payload, key=key)
        self.table[request_id] = {"to": destination, "label": label or key}
        return request_id

    def call(self, context, definition, state, payload, *, key, label=None):
        """Spawn a child and request it in the same checkpoint; returns the child ID."""
        child = context.spawn(definition, state, key=key)
        self.request(context, child, payload, key=key, label=label)
        return child

    def ingest(self, mail):
        """Split delivered mail into matched replies and everything else."""
        replies, others = [], []
        for event in mail:
            pending = None
            if event.get("kind") == "result":
                pending = self.table.get(event.get("request_id"))
            if pending is not None and event.get("source") == pending["to"]:
                del self.table[event["request_id"]]
                replies.append(Reply(event["request_id"], pending["label"], pending["to"],
                                     event.get("payload"), event))
            else:
                others.append(event)
        return replies, others

    def drop(self, destination):
        """Forget every exchange addressed to a session; returns their labels."""
        dropped = [request_id for request_id, pending in self.table.items()
                   if pending["to"] == destination]
        labels = [self.table.pop(request_id)["label"] for request_id in dropped]
        return labels

    @property
    def pending(self):
        return dict(self.table)

    def __len__(self):
        return len(self.table)

    def __bool__(self):
        return bool(self.table)
