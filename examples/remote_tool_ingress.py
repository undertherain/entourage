"""
Transport-neutral ingress — a remote tool call that parks and resumes.

The graph never learns what the transport is. A node dispatches a command
and returns a plan that parks on the *derived await conversation*
``corr:{correlation_id}``; whatever delivers the result — Astral subject,
HTTP webhook, Redis stream, or the fake thread below — reduces it to a
normalized ``InboundEvent`` and hands it to the ``IngressRouter``. The
router resolves the destination at ingress:

    explicit route → live corr:{id} waiter → transport hint → default inbox

so the same event that resumes a parked join today would, arriving after
the wait gave up, fall through to the resident inbox as ambient instead of
stranding (no registration, no cancellation protocol — the await's route
is derived from who is actually waiting).

Two acts:

  1. the result arrives in time  → the parked join resumes, "inline call";
  2. the result arrives too late → the join timed out and moved on; the
     late result lands in the default inbox for the next resident turn.

Run:  python examples/remote_tool_ingress.py
"""

import logging
import threading
import time

from entourage.flow import Sequence, WaitForMailbox
from entourage.ingress import InboundEvent, IngressRouter, correlation_conversation
from entourage.runtime import Runtime

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")


class FakeRemoteService:
    """Stands in for any transport: takes a command, replies via the router.

    A real adapter (Astral subscription, webhook endpoint, poller) does
    exactly this — normalize what it received into an InboundEvent and
    call ``router.accept``, acknowledging its transport only after accept
    returns durably. Swapping this class for a webhook changes nothing in
    the graph, the tool, or the router.
    """

    def __init__(self, router, latency):
        self.router = router
        self.latency = latency

    def submit(self, command):
        threading.Thread(target=self._work, args=(command,)).start()

    def _work(self, command):
        time.sleep(self.latency)
        delivered_to = self.router.accept(InboundEvent(
            event_id=f"result-{command['correlation_id']}",
            kind="result",
            source="weather-service",
            payload={"forecast": "rain in Tokyo"},
            correlation_id=command["correlation_id"],
        ))
        print(f"    (transport delivered the result → {delivered_to!r})")


def make_nodes(service, correlation, join_timeout):
    def dispatch(state):
        print(f"    dispatch: sending command, will join on corr:{correlation}")
        service.submit({"command": "get_weather", "correlation_id": correlation})
        plan = Sequence(
            WaitForMailbox(
                conversation=correlation_conversation(correlation),
                timeout=join_timeout,
            ),
            report,
        )
        return {**state, "dispatched": True}, plan

    def report(state):
        (event,) = state["events"]
        if event["kind"] == "result":
            print(f"    report: {event['payload']['forecast']} "
                  f"(as if it were an inline tool call)")
        else:
            print("    report: remote tool did not answer in time — moving on")
        return state

    return dispatch


def run_act(latency, join_timeout, correlation):
    rt = Runtime()
    router = IngressRouter(
        rt.mailbox,
        default_conversation="agent:inbox",
        waiting_conversations=rt.waiting_conversations,
        wake=rt.wake_due_waits,
    )
    service = FakeRemoteService(router, latency=latency)
    rt.start_session(Sequence(make_nodes(service, correlation, join_timeout)))
    rt.run()
    time.sleep(max(0.0, latency + 0.2))   # let a straggler result land
    leftover = rt.mailbox.claim("agent:inbox", consumer="probe")
    if leftover:
        print(f"    inbox: late {leftover[0]['kind']!r} event waits for the "
              f"next resident turn ({leftover[0]['payload']['forecast']!r})")
    else:
        print("    inbox: empty — the result was consumed by the join")


print("Act 1 — the result beats the join timeout (fast remote):")
run_act(latency=0.2, join_timeout=5.0, correlation="op-fast")

print("\nAct 2 — the join gives up first; the late result falls through:")
run_act(latency=1.0, join_timeout=0.3, correlation="op-slow")
