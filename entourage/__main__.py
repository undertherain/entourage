"""One question through the session dispatcher: `python -m entourage "..."`.

A throwaway store, one chat session, a search tool, and a `ui` session that
prints the answer. Needs a model key and TAVILY_API_KEY; --help needs nothing.
"""

import argparse
import logging
import tempfile
from pathlib import Path

from dotenv import load_dotenv

from .executables import Dispatcher, Executable
from .sessions import LocalSessions
from .tools import TavilySearchTool
from .turn import ChatAgent, litellm_complete


def print_answer(context, state, mail):
    for event in mail:
        print(event["payload"]["text"])
    return context.propose(state, incorporated=[e["event_id"] for e in mail], complete=True)


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("question", nargs="?", default="what's the weather in Tokyo?")
    parser.add_argument("--model", default="gpt-5-nano")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO if args.debug else logging.WARNING)
    with tempfile.TemporaryDirectory() as folder:
        store = LocalSessions(Path(folder) / "sessions.db")
        agent = ChatAgent(litellm_complete(args.model), [TavilySearchTool()], output="ui")
        dispatcher = (Dispatcher(store, lease_seconds=120)
                      .register(Executable("chat:v1", agent.resume))
                      .register(Executable("ui:v1", print_answer)))
        dispatcher.create("ui", "ui:v1", {})
        dispatcher.create("chat", "chat:v1", {"messages": []})
        store.append("chat", {"event_id": "q", "kind": "user", "payload": {"text": args.question}})
        for result in dispatcher.run_until_idle():
            if not result.committed:
                raise SystemExit(f"{result.session_id}: {result.error}")


if __name__ == "__main__":
    main()
