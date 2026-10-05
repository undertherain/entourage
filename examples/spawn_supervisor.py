"""Children as sessions: fork-join, a supervisor loop, and impatience.

Everything here runs on the session dispatcher in one process over a
temporary SQLite store. A child is a session spawned in the parent's
checkpoint; its reply is mail to the parent; the parent keeps the pending
exchanges in state (`entourage.exchanges`) and stays interruptible while it
waits. Nothing is a graph, and no process is launched.

Three acts:

  1. fork-join  — spawn a child and request it in one commit, park, join on
     its reply like a tool return;
  2. supervisor — two children report into the parent; one dies, and the
     failure notice (the runner's, here invoked directly) is ordinary mail
     the loop incorporates before deciding whether to keep waiting;
  3. impatience — a request to a session nobody serves, with a deadline on
     the wait; the timer wakes the parent, which escalates.

Run:  python examples/spawn_supervisor.py
"""

import tempfile
import time
from pathlib import Path

from entourage.exchanges import Exchanges
from entourage.executables import Dispatcher, Executable
from entourage.runner import notify_failures
from entourage.sessions import LocalSessions


def incorporated(mail):
    return [event["event_id"] for event in mail]


# ── Act 1: fork-join ─────────────────────────────────────────

def fork(context, state, mail):
    exchanges = Exchanges(state)
    replies, _ = exchanges.ingest(mail)
    if state.get("phase", "start") == "start":
        child = exchanges.call(context, "resize:v1", {}, {"image": "cat.png"}, key="job-1")
        print(f"    fork: spawned {child}, parking until it replies")
        state["phase"] = "joining"
        return context.propose(state, incorporated=incorporated(mail))
    for reply in replies:
        print(f"    join: child reported {reply.payload['thumbnail']!r}; continuing the turn")
    return context.propose(state, incorporated=incorporated(mail), complete=not exchanges)


def resize(context, state, mail):
    for request in mail:
        print(f"    child: resizing {request['payload']['image']} for {request['reply_to']}")
        context.reply(request, {"thumbnail": "cat_64px.png"})
    return context.propose(state, incorporated=incorporated(mail), complete=True)


# ── Act 2: the supervisor loop ───────────────────────────────

def supervise(context, state, mail):
    exchanges = Exchanges(state)
    if state.get("phase", "start") == "start":
        for slot, definition in (("ok", "ok-worker:v1"), ("doomed", "doomed-worker:v1")):
            exchanges.call(context, definition, {}, {"slot": slot}, key=slot)
        print("    supervisor: spawned ok and doomed workers, parking")
        state["phase"] = "waiting"
        return context.propose(state, incorporated=incorporated(mail))
    replies, others = exchanges.ingest(mail)
    for reply in replies:
        print(f"    supervisor: {reply.label!r} finished fine (work={reply.payload['work']!r})")
    for event in others:
        failed = event.get("payload", {}).get("failed")
        if event["kind"] == "system" and failed:
            labels = exchanges.drop(failed)
            print(f"    supervisor: death notice for {labels!r} after "
                  f"{event['payload']['attempts']} attempt(s); could respawn, logging instead")
    if exchanges:
        print(f"    supervisor: {len(exchanges)} child(ren) outstanding, parking again")
        return context.propose(state, incorporated=incorporated(mail))
    print("    supervisor: all children accounted for, loop ends")
    return context.propose(state, incorporated=incorporated(mail), complete=True)


def ok_worker(context, state, mail):
    for request in mail:
        context.reply(request, {"work": "done"})
    return context.propose(state, incorporated=incorporated(mail), complete=True)


def doomed_worker(context, state, mail):
    raise RuntimeError("segfault in the C extension")


# ── Act 3: impatience ────────────────────────────────────────

def dispatch_into_the_void(context, state, mail):
    exchanges = Exchanges(state)
    if state.get("phase", "start") == "start":
        exchanges.request(context, "the-void", {"job": 9}, key="job-9")
        print("    dispatch: requested a session nobody serves; waiting at most 0.3s")
        state["phase"] = "waiting"
        return context.propose(state, incorporated=incorporated(mail), deadline=time.time() + 0.3)
    replies, others = exchanges.ingest(mail)
    for event in others:
        if event["kind"] == "system" and event.get("source") == "timer":
            print(f"    dispatch: timer woke us with {list(exchanges.pending.values())[0]['label']!r} "
                  "still pending; time to escalate or retry")
    return context.propose(state, incorporated=incorporated(mail), complete=True)


def run(dispatcher):
    for result in dispatcher.run_until_idle():
        if not result.committed:
            print(f"    [{result.session_id} failed: {result.error}]")


def main():
    with tempfile.TemporaryDirectory() as folder:
        store = LocalSessions(Path(folder) / "sessions.db")
        # A short lease keeps the doomed worker's retry backoff short as well.
        dispatcher = (Dispatcher(store, max_attempts=1, lease_seconds=0.5)
                      .register(Executable("fork:v1", fork))
                      .register(Executable("resize:v1", resize))
                      .register(Executable("supervisor:v1", supervise))
                      .register(Executable("ok-worker:v1", ok_worker))
                      .register(Executable("doomed-worker:v1", doomed_worker))
                      .register(Executable("void-caller:v1", dispatch_into_the_void)))
        store.bind_definition("void:v1", {})  # a definition no dispatcher serves

        print("Act 1 — fork-join: spawn a child, park on its reply, join:")
        dispatcher.create("fork", "fork:v1", {})
        run(dispatcher)

        print("\nAct 2 — supervision: two children, one dies; the loop re-decides:")
        dispatcher.create("supervisor", "supervisor:v1", {})
        run(dispatcher)
        time.sleep(0.6)  # the doomed worker's backoff passes; its next claim parks it failed
        run(dispatcher)
        notify_failures(store, "supervisor")  # what the shard runner does on its tick
        run(dispatcher)

        print("\nAct 3 — a request nobody answers; the deadline wakes the waiter:")
        store.create("the-void", "void:v1", {})
        dispatcher.create("caller", "void-caller:v1", {})
        run(dispatcher)
        time.sleep(0.35)
        run(dispatcher)


if __name__ == "__main__":
    main()
