"""Chat loop on the session dispatcher: the conversation is session state.

Each turn is one activation. Tool results are checkpointed before the next
model call, so a chat survives a restart mid-turn and `store.inspect` shows
the whole history. Answers are mail to a `ui` session whose handler prints
them. `/new` starts a fresh session; long-term memory stays in a file.
Use --model for your configured provider; --help makes no model calls.
"""

import argparse
import logging
import os
import uuid
from pathlib import Path

from dotenv import load_dotenv

from entourage.executables import Dispatcher, Executable
from entourage.memory import MemoryDB
from entourage.sessions import LocalSessions
from entourage.tools import MemoryTool, TavilySearchTool
from entourage.turn import ChatAgent, litellm_complete

AGENT = "jarvis:v1"
UI = "cli-ui:v1"

GUIDELINES = """
You are a conversational AI. Follow all instructions below precisely.
---
### CORE INSTRUCTIONS [EN]
- **Primary Goal:** Act as a helpful and wise conversational partner based on the persona defined below.
- **CRITICAL RULE:** You must NEVER break character.
- **Safety:** Decline any harmful or inappropriate requests.
- You have access to a set of tools to help you perform tasks and answer questions.
- Use your tools when you need to fetch external information or perform specific tasks like remembering user details.
- After using a tool, it is critical that you proceed to fully address the user's original request, synthesizing the tool's output into your final answer. Do not get distracted by the tool-use process.
"""
PERSONA = "You are Jarvis, a helpful assistant to Sasha."
USER_NAME = "Aleksandr"


def system_prompt(memory_db):
    def build(context, state):
        facts = [fact.split("] ", 1)[1] for fact in memory_db.get_all() if "] " in fact]
        memory = ""
        if facts:
            memory = f"\n\nHere are facts you remember about {USER_NAME}:\n" + "\n".join(
                f"- {fact}" for fact in facts)
        return f"{PERSONA}\n\n{GUIDELINES}{memory}".strip()
    return build


def print_answers(context, state, mail):
    """The output adapter: a session whose only job is to deliver answers."""
    for event in mail:
        print(f"assistant: {event['payload']['text']}")
    return context.propose(state, incorporated=[event["event_id"] for event in mail])


def show_recent(messages, debug):
    for message in messages[-10:]:
        role, content = message.get("role"), message.get("content")
        if role == "system":
            continue
        if role == "tool" and not debug:
            content = "[Tool output hidden]"
        elif role == "assistant" and message.get("tool_calls") and not debug:
            content = f"[Tool call: {message['tool_calls'][0]['function']['name']}]"
        if content:
            print(f"{role}: {content}")


def latest_session(store):
    open_sessions = [s for s in store.list_sessions(executable=AGENT) if s["status"] != "complete"]
    return open_sessions[-1]["session_id"] if open_sessions else None


def new_session(dispatcher):
    session = f"chat:{uuid.uuid4().hex[:8]}"
    dispatcher.create(session, AGENT, {"messages": []})
    print(f"[Started new chat {session}]")
    return session


def run_ready(dispatcher):
    for result in dispatcher.run_until_idle():
        if not result.committed:
            print(f"[Activation failed for {result.session_id}: {result.error}]")


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", default="claude-3-haiku-20240307", help="Model name to use")
    parser.add_argument("--base-url", default=None, help="Base URL for the API")
    parser.add_argument("--debug", action="store_true", help="Show tool calls and runtime logs")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO if args.debug else logging.WARNING,
                        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    base_dir = Path(os.path.expanduser("~/.entourage/jarvis"))
    base_dir.mkdir(parents=True, exist_ok=True)
    store = LocalSessions(base_dir / "sessions.db")
    memory_db = MemoryDB(base_dir / "memory.txt")

    agent = ChatAgent(litellm_complete(args.model, base_url=args.base_url),
                      [TavilySearchTool(), MemoryTool(memory_db)],
                      system_prompt=system_prompt(memory_db), output="ui")
    # One lease must cover a model call plus its tools; there is no renewal.
    dispatcher = Dispatcher(store, lease_seconds=120)
    dispatcher.register(Executable(AGENT, agent.resume, development=True))
    dispatcher.register(Executable(UI, print_answers, development=True))
    if not store.list_sessions(executable=UI):
        dispatcher.create("ui", UI, {})

    session = latest_session(store)
    if session:
        print(f"[Loaded chat {session}]")
        show_recent(store.inspect(session)["state"].get("messages", []), args.debug)
        run_ready(dispatcher)  # finish a turn interrupted by the previous exit
    else:
        session = new_session(dispatcher)

    while True:
        try:
            text = input("> ")
        except EOFError:
            return
        if not text.strip():
            continue
        if text.strip() == "/new":
            session = new_session(dispatcher)
            continue
        store.append(session, {"event_id": f"user:{uuid.uuid4().hex}", "kind": "user",
                               "payload": {"text": text}})
        run_ready(dispatcher)


if __name__ == "__main__":
    main()
