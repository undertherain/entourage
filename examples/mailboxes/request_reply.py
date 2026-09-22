"""Two coarse-grained agents that release compute while a tool is pending.

Run with --help; walkthroughs live in docs/mailbox-tool-examples.md.
No model, network, graph, or resident agent process is required.
"""

import argparse
import json
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from entourage.sessions import LocalSessions, Publication


def send(target, exchange, step, kind, **payload):
    # Stable per-operation identity: a retried activation proposes the same mail.
    return Publication(target, {
        "event_id": f"{exchange}:{step}", "exchange": exchange,
        "kind": kind, **payload,
    })


def agent(session_id, state, events):
    """One bounded activation; the runtime owns the lease and the commit."""
    state = dict(state)
    exchange = state["exchange"]
    outgoing = []
    incorporated = []
    if state["phase"] == "start":
        outgoing.append(send(
            state["tool"], exchange, "request", "request",
            reply_to=session_id, city=state["city"],
        ))
        state["phase"] = "waiting_for_tool"
    for event in events:
        if event.get("exchange") != exchange:
            raise ValueError("unexpected exchange")
        if event["kind"] == "question":
            outgoing.append(send(state["ui"], exchange, "question", "question",
                                 content=event["content"]))
            state["phase"] = "waiting_for_user"
        elif event["kind"] == "answer":
            outgoing.append(send(state["tool"], exchange, "answer", "answer",
                                 content=event["content"]))
            state["phase"] = "waiting_for_tool"
        elif event["kind"] == "result":
            state["result"] = event["content"]
            state["phase"] = "done"
            outgoing.append(send(state["ui"], exchange, "result", "result",
                                 content=event["content"]))
        else:
            raise ValueError("unexpected agent input")
        incorporated.append(event["event_id"])
    # Remain parked after the result, so later mail need not hit a terminal session.
    return state, incorporated, tuple(outgoing)


def tool(session_id, state, events):
    """Mock lookup, or a mock tour tool that can itself park for clarification."""
    state = dict(state)
    outgoing = []
    incorporated = []
    for event in events:
        exchange = event["exchange"]
        if event["kind"] == "request":
            state.update(exchange=exchange, reply_to=event["reply_to"],
                         city=event["city"])
            if state["scenario"] == "lookup":
                content = f"Mock weather for {state['city']}: sunny, 24 C."
                outgoing.append(send(state["reply_to"], exchange, "result",
                                     "result", content=content))
                state["phase"] = "done"
            else:
                outgoing.append(send(state["reply_to"], exchange, "question",
                                     "question", content="Morning or afternoon tour?"))
                state["phase"] = "waiting_for_answer"
        elif (event["kind"] == "answer"
              and state.get("exchange") == exchange
              and state["phase"] == "waiting_for_answer"):
            content = f"Mock tour confirmed in {state['city']}: {event['content']}."
            outgoing.append(send(state["reply_to"], exchange, "result", "result",
                                 content=content))
            state["phase"] = "done"
        else:
            raise ValueError("unexpected tool input")
        incorporated.append(event["event_id"])
    return state, incorporated, tuple(outgoing)


# Example-only dispatch table, not a durable executable registry or launcher.
HANDLERS = {"mail-demo-agent:v1": agent, "mail-demo-tool:v1": tool}


def tick(store):
    for executable, handler in HANDLERS.items():
        activation = store.claim(executable=executable)
        if activation is None:
            continue
        state, incorporated, outgoing = handler(
            activation.session_id, activation.state, activation.events,
        )
        store.commit(activation, state, incorporated=incorporated, publish=outgoing)
        print(f"{activation.session_id}: {state['phase']}; committed and released.")
        return
    print("No runnable agent or tool. Safe to exit.")


def show(database):
    # Observation never claims or acknowledges the agent's inbox. The UI mailbox
    # is a durable demo sink, read repeatedly rather than delivered to a transport.
    with sqlite3.connect(database) as db:
        for session_id, state in db.execute(
                "SELECT id, state FROM wake_sessions ORDER BY id"):
            print(f"{session_id}: {state}")
        for target, event in db.execute(
                "SELECT session_id, event FROM wake_inputs ORDER BY seq"):
            if target.endswith("/ui"):
                print(f"{target}: {json.loads(event)['content']}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("command", choices=("start", "tick", "answer", "show"))
    parser.add_argument("text", nargs="?")
    parser.add_argument("--scenario", choices=("lookup", "clarify"), default="lookup")
    parser.add_argument("--trip", default="kyoto")
    args = parser.parse_args()
    if args.command == "answer" and not args.text:
        parser.error("answer requires text")
    store = LocalSessions(args.database)
    trip = args.trip
    if args.command == "start":
        # Pre-provision addresses: atomic dynamic child creation is future work.
        store.create(f"{trip}/ui", "mail-demo-ui:v1", {})
        store.create(f"{trip}/tool", "mail-demo-tool:v1",
                     {"scenario": args.scenario, "phase": "idle"})
        store.create(trip, "mail-demo-agent:v1", {
            "phase": "start", "city": trip, "exchange": f"{trip}/call-1",
            "tool": f"{trip}/tool", "ui": f"{trip}/ui",
        })
        print("Addresses created. Run tick to activate the agent.")
    elif args.command == "tick":
        tick(store)
    elif args.command == "answer":
        # One answer per exchange in this demo; retries of delivery deduplicate.
        publication = send(trip, f"{trip}/call-1", "user-answer", "answer",
                           content=args.text)
        store.append(publication.session_id, publication.event)
        print("Answer persisted. The runtime can wake the agent on its next tick.")
    else:
        show(args.database)


if __name__ == "__main__":
    main()
