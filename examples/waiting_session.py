"""
WaitForMailbox — a session that parks durably and wakes on mail or timer.

``WaitForMailbox`` is a plan leaf: wherever a node could go, a wait can
go. The execution parks in the graph store (status ``waiting``, holding no
worker) and wakes when its conversation has claimable events or its
timeout fires. Drained events land in the successor's state under
``"events"`` and are acknowledged inside the same transition commit that
completes the wait; a timeout delivers a single ``kind: system`` timer
event instead, so the successor re-decides rather than hanging forever.

Three acts:

  1. mail is already there  → the wait drains it without parking;
  2. mail arrives later     → the session parks, then wakes on the append;
  3. nothing ever arrives   → the timer event wakes it past the timeout.

Run:  python examples/waiting_session.py
"""

import logging
import threading
import time

from entourage.flow import Sequence, WaitForMailbox
from entourage.runtime import Runtime

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")


def triage(state):
    for event in state["events"]:
        if event["kind"] == "system" and event.get("source") == "timer":
            print(f"    triage: no mail after {event['payload']['timeout']}s — timer woke us")
        else:
            print(f"    triage: got {event['kind']} event: {event['content']!r}")
    return state


def plan():
    return Sequence(WaitForMailbox(conversation="support:alice", timeout=3), triage)


def show_status(rt, session_id, note):
    session = rt.store.get_session(session_id)
    waiting = rt.store.get_session_executions(session_id, status="waiting")
    print(f"    [{note}] session={session['status']}"
          f" parked={[ex['node_name'] for ex in waiting] or 'no'}")


# ── Act 1: mail waiting before the session starts ────────────

print("Act 1 — the event is already in the mailbox, the wait never parks:")
rt = Runtime()
rt.mailbox.append("support:alice", {"kind": "user", "content": "my printer is on fire"})
sid = rt.start_session(plan())
rt.run()
show_status(rt, sid, "after run")

# ── Act 2: park now, mail arrives later ──────────────────────

print("\nAct 2 — nothing to drain yet: the session parks, then mail wakes it:")
rt = Runtime()
sid = rt.start_session(plan())
rt.run_once(poll_wait=0.01)          # executes the wait → parks
show_status(rt, sid, "parked")

def late_interjection():
    time.sleep(0.3)
    print("    (0.3s later, another process appends to the mailbox...)")
    rt.mailbox.append("support:alice", {"kind": "user", "content": "nevermind, fixed it"})

threading.Thread(target=late_interjection).start()
rt.run()                              # the wake tick picks the append up
show_status(rt, sid, "after run")

# ── Act 3: nobody writes — the timer is the fourth wake source ─

print("\nAct 3 — silence: the timeout delivers a kind:system timer event:")
rt = Runtime()
sid = rt.start_session(
    Sequence(WaitForMailbox(conversation="support:alice", timeout=0.5), triage)
)
rt.run_once(poll_wait=0.01)
show_status(rt, sid, "parked")
rt.run()                              # the loop sleeps until wake_at, then fires
show_status(rt, sid, "after run")
