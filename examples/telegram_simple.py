"""Small persistent Telegram conversation bot.

Every Telegram update is pushed into Entourage as a ``telegram_message``
trigger.  The listener does not call the model and Telegram is not treated as
conversation storage; the workflow records both sides in local ChatHistory.

Run everything locally (the queue is in memory, graph/history are on disk):

    TELEGRAM_BOT_TOKEN=... python3 -m examples.telegram_simple

Set ``TELEGRAM_ALLOWED_CHAT_IDS`` to a comma-separated allowlist.  The demo
fails closed when it is unset.  ``/new`` clears the live model context for the
chat; it does not delete the old JSON history file.
"""

import datetime
import logging
import os
import shutil
import threading
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI

from entourage.flow import Sequence
from entourage.integrations.telegram import TelegramListener, TelegramSender
from entourage.memory import ChatHistory
from entourage.runtime import InMemoryReadyQueue, QueueRuntime, SQLiteGraphStore

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
log = logging.getLogger("telegram-demo")

DATA_DIR = Path(os.environ.get("TELEGRAM_DEMO_DATA_DIR", "data/telegram-demo"))
HISTORY_DIR = DATA_DIR / "history"
DB_PATH = DATA_DIR / "entourage.db"
SYSTEM_PROMPT = os.environ.get(
    "TELEGRAM_SYSTEM_PROMPT",
    "You are a helpful personal assistant. Reply concisely and naturally.",
)


def _history(chat_id: str) -> ChatHistory:
    # Telegram chat IDs contain only an optional leading minus and digits.
    return ChatHistory(chat_id, HISTORY_DIR)


def record_inbound(state):
    history = _history(state["chat_id"])
    if state["text"].strip() == "/new":
        archive_dir = HISTORY_DIR / "archive"
        archive_dir.mkdir(parents=True, exist_ok=True)
        if history.file_path.exists():
            stamp = datetime.datetime.now().strftime("%Y%m%dT%H%M%S%f")
            shutil.copy2(history.file_path, archive_dir / f"{state['chat_id']}-{stamp}.json")
        history.set_messages([])
        state["is_command"] = True
        state["response"] = "Started a fresh conversation."
        return state

    history.append({
        "role": "user",
        "content": state["text"],
        "telegram_message_id": state["message_id"],
        "telegram_update_id": state["update_id"],
        "sender": state["sender"],
    })
    state["is_command"] = False
    return state


def respond(state):
    if state["is_command"]:
        return state

    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    messages.extend(
        {"role": m["role"], "content": m["content"]}
        for m in _history(state["chat_id"]).get_messages()[-40:]
        if m.get("role") in {"user", "assistant"} and m.get("content")
    )
    client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
    result = client.chat.completions.create(
        model=os.environ.get("RESPONSE_MODEL", "gpt-4o-mini"),
        messages=messages,
    )
    state["response"] = result.choices[0].message.content
    return state


def send(state):
    # Telegram returns its Message object.  Record the assistant turn only
    # after delivery succeeded, since the bot will not receive its own reply
    # later through getUpdates.
    result = TelegramSender().send(state["chat_id"], state["response"])
    sent = result["result"]
    _history(state["chat_id"]).append({
        "role": "assistant",
        "content": state["response"],
        "telegram_message_id": sent["message_id"],
    })
    return state


def telegram_pipeline(_state):
    return Sequence("record_inbound", "respond", "send")


def create_runtime() -> QueueRuntime:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    runtime = QueueRuntime(
        store=SQLiteGraphStore(DB_PATH),
        queue=InMemoryReadyQueue(),
    )
    runtime.register_node("record_inbound", record_inbound)
    runtime.register_node("respond", respond)
    runtime.register_node("send", send, max_attempts=3, retry_delay=2)
    runtime.register_pipeline("telegram_message", telegram_pipeline)
    return runtime


def run_listener(runtime: QueueRuntime):
    def enqueue(message):
        runtime.send_trigger(
            "telegram_message",
            message,
            serial_key=f"telegram:{message['chat_id']}",
        )
        log.info("queued update %s from chat %s", message["update_id"], message["chat_id"])

    TelegramListener(on_message=enqueue).run()


def main():
    runtime = create_runtime()
    # SQLiteGraphStore's connection belongs to this main thread.  The listener
    # only calls the thread-safe in-memory queue, so it runs in the background.
    listener = threading.Thread(target=run_listener, args=(runtime,), daemon=True)
    listener.start()
    runtime.run()


if __name__ == "__main__":
    main()
