"""Codex-like mailbox checkpoint demo (no model or external services).

Type while the background loop is working. New events are durable only in
memory for this demo and join the active work at named safe checkpoints.

Commands:
    /subagent TEXT  inject a typed subagent update
    /ambient TEXT   inject an ambient alert (for example, Grafana)
    /quit           stop after the current checkpoint
"""

import threading
import time

from entourage.mailbox import InMemoryMailbox


CONVERSATION = "demo"
CONSUMER = "demo-agent"


def render(event):
    kind = event.get("kind", "user")
    return f"{kind}: {event.get('content', '')}"


def drain(mailbox, checkpoint):
    events = mailbox.claim(CONVERSATION, CONSUMER, limit=20)
    if not events:
        print(f"\n[checkpoint: {checkpoint}; no new events]", flush=True)
        return []
    print(f"\n[checkpoint: {checkpoint}; ingesting {len(events)} event(s)]", flush=True)
    for event in events:
        print(f"  + {render(event)}", flush=True)
    mailbox.acknowledge(
        CONVERSATION, CONSUMER, [event["event_id"] for event in events]
    )
    return events


def agent_loop(mailbox, stop):
    while not stop.is_set():
        if not mailbox.wait_for_events(CONVERSATION, timeout=0.2):
            continue
        events = drain(mailbox, "before model")
        if not events:
            continue
        print("agent: inspecting the request (type another message now)", flush=True)
        time.sleep(2)
        drain(mailbox, "after inspect")
        print("agent: running a pretend tool/subagent", flush=True)
        time.sleep(2)
        drain(mailbox, "after tool")
        print("agent: composing the answer", flush=True)
        time.sleep(1)
        joined = drain(mailbox, "before final answer")
        if joined:
            print("agent: incorporated the late update before answering", flush=True)
        print("agent: done; waiting for more events\n> ", end="", flush=True)


def main():
    mailbox = InMemoryMailbox()
    stop = threading.Event()
    worker = threading.Thread(target=agent_loop, args=(mailbox, stop), daemon=True)
    worker.start()
    print("Mailbox checkpoint demo. Type a request, /subagent ..., /ambient ..., or /quit.")
    while True:
        try:
            text = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            text = "/quit"
        if not text:
            continue
        if text == "/quit":
            stop.set()
            break
        if text.startswith("/subagent "):
            kind, content = "subagent", text[len("/subagent "):]
        elif text.startswith("/ambient "):
            kind, content = "ambient", text[len("/ambient "):]
        else:
            kind, content = "user", text
        mailbox.append(CONVERSATION, {"kind": kind, "content": content, "source": "cli"})
    worker.join(timeout=0.5)


if __name__ == "__main__":
    main()
