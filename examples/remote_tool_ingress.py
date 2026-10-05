"""A remote tool call that parks and resumes, and a result that arrives late.

The session is the return address. A handler stages a request with
`reply_to` set to its own session, parks with a deadline, and whatever
delivers the result (an Astral subject, a webhook, a poller, or the fake
thread below) appends it to that session. No routing table: the exchange
table in state (`entourage.exchanges`) tells a matched reply from anything
else, so a result arriving after the waiter gave up is ordinary mail for
the next turn instead of a stranded message.

Two acts:

  1. the result arrives in time  → the reply resumes the parked turn, as
     if the call had been inline;
  2. the result arrives too late  → the deadline woke the session first,
     it dropped the exchange and moved on; the late result lands as
     ambient mail on the next wake.

Run:  python examples/remote_tool_ingress.py
"""

import tempfile
import threading
import time
from pathlib import Path

from entourage.exchanges import Exchanges
from entourage.executables import Dispatcher, Executable
from entourage.sessions import LocalSessions


class FakeRemoteService:
    """Stands in for any transport: takes a request event, replies by append.

    A real adapter must append to durable storage before acknowledging its
    transport; `LocalSessions.append` is that durable, idempotent step.
    """

    def __init__(self, store, latency):
        self.store = store
        self.latency = latency

    def submit(self, request):
        threading.Thread(target=self._work, args=(request,)).start()

    def _work(self, request):
        time.sleep(self.latency)
        self.store.append(request["reply_to"], {
            "event_id": f"result:{request['request_id']}", "kind": "result",
            "source": "weather-service", "request_id": request["request_id"],
            "payload": {"forecast": "rain in Tokyo"},
        })
        print("    (transport delivered the result to the session)")


def make_agent(join_timeout):
    def resume(context, state, mail):
        exchanges = Exchanges(state)
        incorporated = [event["event_id"] for event in mail]
        if state.get("phase", "start") == "start":
            exchanges.request(context, "weather-service", {"command": "get_weather"},
                              key="op", label="forecast")
            print(f"    dispatch: requested the forecast, waiting up to {join_timeout}s")
            state["phase"] = "waiting"
            return context.propose(state, incorporated=incorporated,
                                   deadline=time.time() + join_timeout)
        replies, others = exchanges.ingest(mail)
        for reply in replies:
            print(f"    report: {reply.payload['forecast']} (as if it were an inline tool call)")
        for event in others:
            if event["kind"] == "system" and event.get("source") == "timer" and exchanges:
                print(f"    report: no answer in time, dropping {exchanges.drop('weather-service')!r} "
                      "and moving on")
            elif event["kind"] == "result":
                print(f"    inbox: late result {event['payload']['forecast']!r} is ambient mail "
                      "for this turn, not a resumed call")
        return context.propose(state, incorporated=incorporated)
    return resume


def run_act(latency, join_timeout):
    with tempfile.TemporaryDirectory() as folder:
        store = LocalSessions(Path(folder) / "sessions.db")
        service = FakeRemoteService(store, latency)
        # The service session exists only as an address; the fake thread reads its inbox.
        store.bind_definition("weather-service:v1", {})
        store.create("weather-service", "weather-service:v1", {})
        dispatcher = Dispatcher(store).register(Executable("agent:v1", make_agent(join_timeout)))
        dispatcher.create("agent", "agent:v1", {})
        dispatcher.run_until_idle()
        request = store.claim(executable="weather-service:v1").events[0]
        service.submit(request)
        stop = threading.Event()
        threading.Timer(max(latency, join_timeout) + 0.3, stop.set).start()
        dispatcher.run_forever(stop, poll_interval=0.02)


print("Act 1 — the result beats the deadline (fast remote):")
run_act(latency=0.1, join_timeout=1.0)

print("\nAct 2 — the deadline passes first; the late result falls through:")
run_act(latency=0.5, join_timeout=0.1)
