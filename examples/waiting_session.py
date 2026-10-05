"""Three ways a parked session wakes: mail already there, mail later, a timer.

A session on the dispatcher parks with every proposal that is not complete.
It is ready again when its inbox has unincorporated mail or its deadline has
passed, and both facts are durable, so this holds across restarts as well as
within one process. Compare the registered example for the fresh-process
walkthrough.

Run:  python examples/waiting_session.py
"""

import tempfile
import threading
import time
from pathlib import Path

from entourage.executables import Dispatcher, Executable
from entourage.sessions import LocalSessions


def triage(context, state, mail):
    for event in mail:
        if event["kind"] == "system" and event.get("source") == "timer":
            print("    triage: nothing arrived, the timer woke us")
        else:
            print(f"    triage: got {event['kind']} event: {event['payload']['text']!r}")
    state["wakes"] = state.get("wakes", 0) + 1
    deadline = None if mail else time.time() + 0.3  # wait at most 0.3s for the first mail
    return context.propose(state, incorporated=[e["event_id"] for e in mail],
                           deadline=deadline, complete=bool(mail))


def user(text):
    return {"event_id": f"user:{text}", "kind": "user", "payload": {"text": text}}


def main():
    with tempfile.TemporaryDirectory() as folder:
        store = LocalSessions(Path(folder) / "sessions.db")
        dispatcher = Dispatcher(store).register(Executable("triage:v1", triage))

        print("Act 1 — the mail is already there; the first activation sees it:")
        dispatcher.create("alice", "triage:v1", {})
        store.append("alice", user("my printer is on fire"))
        dispatcher.run_until_idle()
        print(f"    [alice is {store.inspect('alice')['status']}]")

        print("\nAct 2 — nothing to read yet: park, then mail wakes the session:")
        dispatcher.create("bob", "triage:v1", {})
        dispatcher.run_until_idle()  # first activation: empty batch, parks with a deadline
        print(f"    [bob is {store.inspect('bob')['status']}, holding no worker]")
        threading.Timer(0.1, lambda: store.append("bob", user("nevermind, fixed it"))).start()
        stop = threading.Event()
        threading.Timer(0.25, stop.set).start()
        dispatcher.run_forever(stop, poll_interval=0.02)
        print(f"    [bob is {store.inspect('bob')['status']}]")

        print("\nAct 3 — silence: the deadline delivers a kind:system timer event:")
        dispatcher.create("carol", "triage:v1", {})
        dispatcher.run_until_idle()
        time.sleep(0.35)
        dispatcher.run_until_idle()
        print(f"    [carol is {store.inspect('carol')['status']} after "
              f"{store.inspect('carol')['state']['wakes']} wakes]")


if __name__ == "__main__":
    main()
