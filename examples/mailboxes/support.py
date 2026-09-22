"""Example-only helpers over LocalSessions, not a new executable runtime.

One agent and one mock tool per demo database. All outgoing mail is staged until
save(); the checkpoint incorporates the whole batch only after handling succeeds.
"""

import argparse
import asyncio
from copy import deepcopy
import json
from pathlib import Path
import sqlite3
import tempfile

from entourage.sessions import LocalSessions, Publication


class Turn:
    def __init__(self, store, activation):
        self.store = store
        self._activation = activation
        self.state = deepcopy(activation.state)
        self.mail = deepcopy(activation.events)
        self._outgoing = []

    def call(self, tool, *, city, key):
        """Stage a request with an automatic reply address and stable call ID."""
        call_id = f"{self._activation.session_id}:{key}"
        self._outgoing.append(Publication(tool, {
            "event_id": call_id, "kind": "request", "call_id": call_id,
            "reply_to": self._activation.session_id, "city": city,
        }))
        return call_id

    def result(self, call_id):
        return next(event["content"] for event in self.mail
                    if event["kind"] == "result" and event["call_id"] == call_id)

    def save(self, *, next_step=None, complete=False):
        if next_step is not None:
            self.state["next"] = next_step.__name__
        self.store.commit(
            self._activation, self.state,
            incorporated=[event["event_id"] for event in self._activation.events],
            publish=tuple(self._outgoing), complete=complete,
        )


def receive(store):
    activation = store.claim(executable="example-agent:v1")
    return Turn(store, activation) if activation else None


def run_once(store, steps):
    """Restore state and dispatch its saved continuation, then return to the CLI."""
    turn = receive(store)
    if turn is None:
        print("No mail; no activation.")
        return
    handlers = {step.__name__: step for step in steps}
    handlers[turn.state["next"]](turn)


async def mailbox(store):
    """Keep the process alive; hold no activation lease while the inbox is empty."""
    while True:
        turn = receive(store)
        if turn is None:
            await asyncio.sleep(0.01)  # local polling, not agent dehydration
        else:
            yield turn


def initialize(store, *, next_step=None):
    store.create("weather", "example-tool:v1", {})
    state = {"next": next_step.__name__} if next_step else {}
    store.create("agent", "example-agent:v1", state)


def reply(store, activation):
    publications = tuple(Publication(event["reply_to"], {
        "event_id": f"{event['call_id']}:result", "kind": "result",
        "call_id": event["call_id"],
        "content": f"Mock weather for {event['city']}: sunny, 24 C.",
    }) for event in activation.events)
    store.commit(activation, {},
                 incorporated=[event["event_id"] for event in activation.events],
                 publish=publications)


async def tool_worker(store):
    while True:
        activation = store.claim(executable="example-tool:v1")
        if activation:
            await asyncio.sleep(0.05)  # pretend inference takes time
            reply(store, activation)
        else:
            await asyncio.sleep(0.01)


async def resident_demo(agent):
    with tempfile.TemporaryDirectory(prefix="entourage-mailbox-") as directory:
        store = LocalSessions(Path(directory) / "demo.db")
        initialize(store)
        store.append("agent", {"event_id": "ask", "kind": "user", "city": "Kyoto"})
        worker = asyncio.create_task(tool_worker(store))

        async def follow_up():
            # Wait until dispatch committed, then send unrelated work while the
            # tool is pending. This deliberately exercises a later mailbox batch.
            while "pending" not in read_state(store):
                await asyncio.sleep(0)
            store.append("agent", {"event_id": "note", "kind": "note",
                                   "content": "Also remember to pack a hat."})

        producer = asyncio.create_task(follow_up())
        try:
            await agent(store)
        finally:
            worker.cancel()
            producer.cancel()
            await asyncio.gather(worker, producer, return_exceptions=True)


def read_state(store):
    with sqlite3.connect(store.path) as db:
        return json.loads(db.execute(
            "SELECT state FROM wake_sessions WHERE id = 'agent'"
        ).fetchone()[0])


def restart_cli(steps):
    parser = argparse.ArgumentParser(description="One durable activation per run.")
    parser.add_argument("database", type=Path)
    parser.add_argument("command", choices=("start", "run", "tool", "show"))
    args = parser.parse_args()
    store = LocalSessions(args.database)
    if args.command == "start":
        initialize(store, next_step=steps[0])
        run_once(store, steps)
    elif args.command == "run":
        run_once(store, steps)
    elif args.command == "tool":
        activation = store.claim(executable="example-tool:v1")
        if activation:
            reply(store, activation)
            print("Tool result delivered to the agent mailbox.")
    else:
        print(json.dumps(read_state(store), indent=2))
