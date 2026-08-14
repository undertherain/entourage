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
from prompt_toolkit import PromptSession
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit.history import InMemoryHistory
from prompt_toolkit.patch_stdout import patch_stdout
from prompt_toolkit.styles import Style


CONVERSATION = "demo"
CONSUMER = "demo-agent"
PROMPT_STYLE = Style.from_dict({
    "frame": "bold ansicyan",
    "hint": "ansibrightblack",
})


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
        print("agent: done; waiting for more events", flush=True)


def parse_input(text):
    if text.startswith("/subagent "):
        return "subagent", text[len("/subagent "):]
    if text.startswith("/ambient "):
        return "ambient", text[len("/ambient "):]
    return "user", text


def main():
    mailbox = InMemoryMailbox()
    stop = threading.Event()
    worker = threading.Thread(target=agent_loop, args=(mailbox, stop), daemon=True)
    worker.start()
    print("Mailbox checkpoint demo. Type a request, /subagent ..., /ambient ..., or /quit.")
    session = PromptSession(history=InMemoryHistory(), style=PROMPT_STYLE)
    with patch_stdout(raw=True):
        while True:
            try:
                text = session.prompt(
                    HTML("<frame>╭─ message\n╰─› </frame>"),
                    bottom_toolbar=HTML(
                        "<hint>Enter send · /subagent update · /ambient alert · /quit</hint>"
                    ),
                ).strip()
            except (EOFError, KeyboardInterrupt):
                text = "/quit"
            if not text:
                continue
            if text == "/quit":
                stop.set()
                break
            kind, content = parse_input(text)
            mailbox.append(
                CONVERSATION, {"kind": kind, "content": content, "source": "cli"}
            )
    worker.join(timeout=0.5)


if __name__ == "__main__":
    main()
