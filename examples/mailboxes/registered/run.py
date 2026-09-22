"""Each CLI invocation constructs a fresh dispatcher over the same database."""

import argparse
import json
from pathlib import Path
from threading import Event

from entourage.executables import Dispatcher, Executable
from entourage.sessions import LocalSessions


HERE = Path(__file__).parent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("command", choices=("start", "tick", "correct", "tool", "show", "serve"))
    parser.add_argument("text", nargs="?", default="Quiet indoor activities in Kyoto")
    args = parser.parse_args()
    store = LocalSessions(args.database)
    if args.command == "show":
        # Observation must work even when executable source is unavailable or changed.
        print(json.dumps({name: store.inspect(name) for name in ("research", "events")}, indent=2))
        return
    dispatcher = Dispatcher(store)
    manifest = "tool.yaml" if args.command == "tool" else "agent.yaml"
    dispatcher.register(Executable.from_manifest(HERE / manifest))
    if args.command == "start":
        tool = Dispatcher(store).register(Executable.from_manifest(HERE / "tool.yaml"))
        # Output is a durable mailbox, consumed by a UI adapter in a real application.
        store.create("ui", "fixture-output:v1", {})
        tool.create("sources", "sources:v1", {})
        dispatcher.create("research", "specialist:v1", {
            "task": "research", "phase": "ready", "brief": "Outdoor activities in Kyoto",
        })
        dispatcher.create("events", "specialist:v1", {"task": "events", "phase": "ready"})
    elif args.command == "correct":
        store.append("research", {"event_id": "correction:1", "kind": "user",
                                  "payload": {"brief": args.text}})
    elif args.command == "serve":
        dispatcher.register(Executable.from_manifest(HERE / "tool.yaml"))
        try:
            dispatcher.run_forever(Event())
        except KeyboardInterrupt:
            pass
        return
    for result in dispatcher.run_until_idle():
        if not result.committed:
            raise RuntimeError(f"activation failed for {result.session_id}") from result.error
        print(f"Committed {result.session_id}")


if __name__ == "__main__":
    main()
