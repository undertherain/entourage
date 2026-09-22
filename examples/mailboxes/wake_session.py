"""A session that releases its process and wakes on a later answer.

Run start, tick, send TEXT, tick as separate invocations against the same DB.
"""

import argparse
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from entourage.sessions import LocalSessions


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    parser.add_argument("command", choices=("start", "send", "tick"))
    parser.add_argument("text", nargs="?")
    args = parser.parse_args()
    if args.command == "send" and not args.text:
        parser.error("send requires answer text")
    store = LocalSessions(args.database)
    if args.command == "send":
        store.append("trip", {"event_id": uuid.uuid4().hex,
                              "kind": "user", "content": args.text})
        print("Answer saved; the next tick can reactivate the session.")
        return
    if args.command == "start":
        store.create("trip", "travel:v1", {"phase": "start"})
    activation = store.claim(executable="travel:v1")
    if activation is None:
        print("No runnable session. Safe to exit.")
        return
    if activation.state["phase"] == "start":
        store.commit(activation, {"phase": "clarify"}, incorporated=[])
        print("How many people? State saved; session parked; process can exit.")
    else:
        answers = [event["content"] for event in activation.events]
        store.commit(activation, {"phase": "done", "answers": answers},
                     incorporated=[event["event_id"] for event in activation.events],
                     complete=True)
        print(f"Restored clarification phase. Answer saved: {', '.join(answers)}")


if __name__ == "__main__":
    main()
