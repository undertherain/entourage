"""Coding chat on the session dispatcher with local file and command tools.

Same shape as examples/cli.py: the conversation is session state, each turn is
one activation and tool results are checkpointed before the next model call.
Use --model for your configured provider; --help makes no model calls.
"""

import argparse
import logging
import os
import uuid
from pathlib import Path

from dotenv import load_dotenv

from entourage.executables import Dispatcher, Executable
from entourage.sessions import LocalSessions
from entourage.tools import ListDirTool, ReadFileTool, RunCommandTool, WriteFileTool
from entourage.turn import ChatAgent, litellm_complete

AGENT = "coderbot:v1"
UI = "cli-ui:v1"

SYSTEM_PROMPT = """You are CoderBot, an expert coding assistant.

You are an expert software engineer AI. Follow all instructions below precisely.
---
### CORE INSTRUCTIONS [EN]
- **Primary Goal:** Assist the user with coding tasks, debugging, and explaining code.
- **Tools:**
  - `list_files`: Use this to explore the directory structure.
  - `read_file`: Use this to read the content of files.
  - `write_file`: Use this to write content to a file. Behavior: overwritten if exists, created if not.
  - `run_command`: Use this to execute shell commands.
- **Analysis:** proper analysis often requires understanding the project structure first, then reading specific files.
- **Verification:** After writing code, ALWAYS verify it by running it with the `run_command` tool (e.g., `python3 filename.py`).
- **Safety:** You have write access. Be careful when overwriting files. Always double-check path.
- **Communication:** Be concise and technical.
""".strip()


def print_answers(context, state, mail):
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

    base_dir = Path(os.path.expanduser("~/.entourage/coderbot"))
    base_dir.mkdir(parents=True, exist_ok=True)
    store = LocalSessions(base_dir / "sessions.db")

    agent = ChatAgent(litellm_complete(args.model, base_url=args.base_url),
                      [ListDirTool(), ReadFileTool(), WriteFileTool(), RunCommandTool()],
                      system_prompt=SYSTEM_PROMPT, output="ui")
    # One lease must cover a model call plus its tools; commands can be slow.
    dispatcher = Dispatcher(store, lease_seconds=300)
    dispatcher.register(Executable(AGENT, agent.resume, development=True))
    dispatcher.register(Executable(UI, print_answers, development=True))
    if not store.list_sessions(executable=UI):
        dispatcher.create("ui", UI, {})

    session = latest_session(store)
    if session:
        print(f"[Loaded chat {session}]")
        show_recent(store.inspect(session)["state"].get("messages", []), args.debug)
        run_ready(dispatcher)
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
