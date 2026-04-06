"""
Simple Telegram bot using Entourage pipeline.

Listens for messages, responds via LLM.

Two processes:
  Terminal 1 (worker):   python3 -m examples.telegram_simple
  Terminal 2 (listener): python3 -m examples.telegram_simple --listen
"""

import argparse
import logging
import os
import threading

from dotenv import load_dotenv
from openai import OpenAI

from entourage.flow import Conditional, Sequence
from entourage.runtime import QueueRuntime
from entourage.integrations.telegram import TelegramListener
from entourage.integrations.telegram.bot import TelegramSender

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
telegram = TelegramSender()


# ── Pipeline nodes ────────────────────────────────────────────


def triage(state):
    """Quick check: should we respond? Uses simple heuristics, no LLM."""
    text = state.get("text", "")
    # Respond to questions or direct mentions
    should_reply = text.strip().endswith("?") or "bot" in text.lower()
    state["should_reply"] = should_reply
    logging.info("Triage: '%s' → should_reply=%s", text[:50], should_reply)
    return state


def respond(state):
    """Generate a response using LLM."""
    messages = [
        {"role": "system", "content": "You are a helpful assistant in a chat. Be concise."},
    ]
    # Add history context
    for m in state.get("history", []):
        if "bot_id" in m:
            messages.append({"role": "assistant", "content": m["text"]})
        else:
            messages.append({"role": "user", "content": f"{m['sender']}: {m['text']}"})

    model = os.environ.get("RESPONSE_MODEL", "gpt-4o-mini")
    response = client.chat.completions.create(model=model, messages=messages)
    state["response"] = response.choices[0].message.content
    logging.info("Generated response: %s", state["response"][:80])
    return state


def send(state):
    """Send the response back to Telegram."""
    telegram.send(state["chat_id"], state["response"])
    logging.info("Sent reply to chat %s", state["chat_id"])
    return state


# ── Pipeline template ─────────────────────────────────────────


def simple_reply_pipeline(state):
    return Sequence(
        "triage",
        Conditional("should_reply", Sequence("respond", "send")),
    )


# ── Runtime setup ─────────────────────────────────────────────


def create_runtime(**kwargs) -> QueueRuntime:
    rt = QueueRuntime(**kwargs)
    rt.register_node("triage", triage)
    rt.register_node("respond", respond)
    rt.register_node("send", send)
    rt.register_pipeline("telegram_message", simple_reply_pipeline)
    return rt


# ── Entry points ──────────────────────────────────────────────

def run_listener(runtime: QueueRuntime):
    """Listen to Telegram and push triggers to SQS."""
    def on_message(msg):
        logging.info("Got message from %s: %s", msg["sender"], msg["text"][:50])
        runtime.send_trigger("telegram_message", msg)

    listener = TelegramListener(on_message=on_message)
    listener.run()


def run_worker(runtime: QueueRuntime):
    """Poll SQS and execute pipeline nodes."""
    logging.info("Worker starting...")
    runtime.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Entourage Telegram Bot")
    parser.add_argument("--listen", action="store_true", help="Run the Telegram listener")
    parser.add_argument("--both", action="store_true", help="Run listener + worker in one process")
    args = parser.parse_args()

    rt = create_runtime()

    if args.both:
        # Run both in one process (convenient for testing)
        worker_thread = threading.Thread(target=run_worker, args=(rt,), daemon=True)
        worker_thread.start()
        run_listener(rt)
    elif args.listen:
        run_listener(rt)
    else:
        run_worker(rt)
