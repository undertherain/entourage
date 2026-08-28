"""
Spawn on the commit — fork-join, a supervisor loop, and a lapsing monitor.

``Transition(spawn=[Spawn(...)])`` creates a child session atomically with
the parent node's completion: deterministic child identity (replay cannot
twin it), lineage in the child's initial state, and the child contract
fulfilled by the engine — a completing child publishes a correlated
``kind: result`` event to its ``notify`` conversation, a failing child a
``kind: system`` death notice. Both are ordinary mail: they feed armed
monitors and wake parked parents.

Three acts:

  1. fork-join  — spawn + park on ``corr:{correlation_id}``; the child's
     result resumes the parent like a tool return;
  2. supervisor — two children report into one inbox; the supervisor loop
     re-parks after every drain until all children are accounted for,
     incorporating one success and one death notice;
  3. monitor    — a dispatch to a remote that never answers, with a
     deadline monitor armed on the same commit; the lapse arrives as
     ``kind: system`` mail and wakes the supervisor.

Run:  python examples/spawn_supervisor.py
"""

import logging
import time

from entourage.flow import Sequence, WaitForMailbox
from entourage.ingress import correlation_conversation
from entourage.monitors import Monitor
from entourage.runtime import Runtime
from entourage.transition import Spawn, Transition

logging.basicConfig(level=logging.CRITICAL, format="%(levelname)s %(message)s")


# ── Act 1: fork-join ─────────────────────────────────────────

print("Act 1 — fork-join: spawn a child, park on its correlation, join:")

def fork(state):
    print("    fork: spawning resize-worker, parking until it reports")
    return Transition(
        state=state,
        spawn=[Spawn(plan=resize, initial_state={"image": "cat.png"},
                     correlation_id="job-1")],
        plan=Sequence(
            WaitForMailbox(conversation=correlation_conversation("job-1")),
            join,
        ),
    )

def resize(state):
    print(f"    child: resizing {state['image']} "
          f"(lineage: parent exec {state['spawn']['parent_exec_id'][:8]}…)")
    return {**state, "thumbnail": "cat_64px.png"}

def join(state):
    (event,) = state["events"]
    print(f"    join: child reported {event['payload']['thumbnail']!r} "
          f"— continuing the original turn")
    return state

rt = Runtime()
rt.start_session(Sequence(fork))
rt.run()


# ── Act 2: the supervisor loop ───────────────────────────────

print("\nAct 2 — supervision: two children, one dies; the loop re-decides:")

def dispatch_fleet(state):
    print("    supervisor: spawning ok-worker and doomed-worker → inbox")
    return Transition(
        state={**state, "outstanding": 2},
        spawn=[
            Spawn(plan=ok_worker, slot="ok", notify="sup:inbox"),
            Spawn(plan=doomed_worker, slot="doomed", notify="sup:inbox"),
        ],
        plan=supervise_plan(),
    )

def supervise_plan():
    return Sequence(WaitForMailbox(conversation="sup:inbox", timeout=5), supervise)

def ok_worker(state):
    return {**state, "work": "done"}

def doomed_worker(state):
    raise RuntimeError("segfault in the C extension")

def supervise(state):
    outstanding = state["outstanding"]
    for event in state["events"]:
        payload = event.get("payload", {})
        # A result's payload is the child's final state; lineage rides it.
        slot = payload.get("slot") or payload.get("spawn", {}).get("slot", "?")
        if event["kind"] == "result":
            print(f"    supervisor: slot {slot!r} finished fine "
                  f"(work={payload['work']!r})")
            outstanding -= 1
        elif event["kind"] == "system" and event.get("payload", {}).get("reason") == "failed":
            print(f"    supervisor: death notice from slot {slot!r}: "
                  f"{event['payload']['error']!r} — could re-spawn, logging instead")
            outstanding -= 1
    new_state = {**state, "outstanding": outstanding}
    if outstanding > 0:
        print(f"    supervisor: {outstanding} child(ren) outstanding — parking again")
        return new_state, supervise_plan()   # the loop: wait → decide → wait
    print("    supervisor: all children accounted for — loop ends")
    return new_state

rt = Runtime()
rt.start_session(Sequence(dispatch_fleet))
rt.run()


# ── Act 3: the monitor covers silence ────────────────────────

print("\nAct 3 — a remote that never answers; the armed monitor lapses:")

def dispatch_into_the_void(state):
    print("    dispatch: publishing command to a dead remote, arming a 0.5s "
          "deadline monitor on the same commit")
    return Transition(
        state=state,
        arm=[Monitor(notify="sup:inbox", correlation_id="job-9",
                     deadline=time.time() + 0.5)],
        plan=Sequence(WaitForMailbox(conversation="sup:inbox", timeout=5), on_lapse),
    )

def on_lapse(state):
    (event,) = state["events"]
    payload = event["payload"]
    print(f"    supervisor: monitor {payload['monitor_id']!r} lapsed "
          f"({payload['reason']}, correlation {payload['correlation_id']!r}) "
          f"— nobody answered; time to escalate or retry")
    return state

rt = Runtime()
rt.start_session(Sequence(dispatch_into_the_void))
rt.run()
