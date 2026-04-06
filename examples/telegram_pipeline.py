"""
Telegram triage → generate → send pipeline.

Ported from refs/workflow to use the new persistent QueueRuntime.

Run the runtime:
    python3 -m examples.telegram_pipeline

Send a trigger (from Telegram service or manually):
    python3 -m examples.telegram_pipeline --trigger "is the moon made of cheese?"
"""

import argparse
import logging
import os

from dotenv import load_dotenv
from openai import OpenAI

from entourage.flow import Conditional, Sequence
from entourage.runtime import QueueRuntime

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))


# ── Node functions ────────────────────────────────────────────
# Each takes state dict, returns state dict (or (state, plan) tuple).
# Same logic as refs/workflow/loop/tools.py, cleaned up.


def triage_message(state):
    """Classify the last message — cheap model, near-zero cost."""
    system_prompt = (
        "You analyze chat messages. Classify the LAST message as:\n"
        "DIRECT_BOT_ADDRESS — explicitly directed at the bot\n"
        "GROUP_QUESTION — a question the bot should answer\n"
        "OTHER — no response needed\n\n"
        "Output only the category label."
    )
    messages = [{"role": "system", "content": system_prompt}]
    messages += _history_to_llm(state)

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
        max_tokens=50,
        temperature=0,
    )
    label = response.choices[0].message.content.strip()
    state["need_to_reply"] = label in ("DIRECT_BOT_ADDRESS", "GROUP_QUESTION")
    logging.info("Triage: %s → need_to_reply=%s", label, state["need_to_reply"])
    return state


def generate_response(state):
    """Generate a reply — heavier model, only called when needed."""
    system_prompt = (
        "You are 'Alexander the Bot', a friendly community chat member. "
        "Helpful, knowledgeable, concise. "
        "Do not mention you are an AI. Do not suggest asking more questions."
    )
    messages = [{"role": "system", "content": system_prompt}]
    messages += _history_to_llm(state)

    response = client.chat.completions.create(
        model=os.environ.get("RESPONSE_MODEL", "gpt-4o-mini"),
        messages=messages,
    )
    state["response"] = response.choices[0].message.content
    return state


def send_message(state):
    """Send reply via the Telegram service."""
    import requests

    bot_id = os.environ.get("TELEGRAM_BOT_ID", "7956302315")
    tg_service_url = os.environ.get("TELEGRAM_SERVICE_URL", "http://127.0.0.1:8000")
    tg_service_token = os.environ.get("TELEGRAM_SERVICE_TOKEN", "myAPIAPIsecret1da")

    payload = {"chat_id": state["chat_id"], "text": state["response"]}
    url = f"{tg_service_url}/send_message/{bot_id}"
    headers = {"Authorization": f"Bearer {tg_service_token}"}

    resp = requests.post(url, json=payload, headers=headers)
    logging.info("Sent message to chat %s: %s", state["chat_id"], resp.status_code)
    return state


def _history_to_llm(state):
    messages = []
    for m in state.get("history", []):
        if "bot_id" in m:
            messages.append({"role": "assistant", "content": m["text"]})
        else:
            messages.append({"role": "user", "content": f"{m['sender']}: {m['text']}"})
    return messages


# ── Pipeline template ─────────────────────────────────────────


def telegram_reply_pipeline(state):
    """
    Pipeline: triage → (if need_to_reply) generate → send

    The condition on the triage→generate edge is handled by the runtime:
    edges with condition="need_to_reply" only fire when state["need_to_reply"] is truthy.
    """
    return Sequence(
        "triage_message",
        Conditional("need_to_reply", Sequence("generate_response", "send_message")),
    )


# ── Runtime setup ─────────────────────────────────────────────


def create_runtime(**kwargs) -> QueueRuntime:
    runtime = QueueRuntime(**kwargs)

    # Register nodes
    runtime.register_node("triage_message", triage_message)
    runtime.register_node("generate_response", generate_response)
    runtime.register_node("send_message", send_message)

    # Register pipeline
    runtime.register_pipeline("telegram_reply", telegram_reply_pipeline)

    return runtime


# ── CLI ───────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--trigger", type=str, help="Send a test trigger message")
    parser.add_argument("--chat-id", type=str, default="-553486639")
    args = parser.parse_args()

    rt = create_runtime()

    if args.trigger:
        # Send a trigger and exit
        rt.send_trigger("telegram_reply", {
            "history": [
                {"sender": "test_user", "text": args.trigger}
            ],
            "chat_id": args.chat_id,
        })
        print(f"Trigger sent: {args.trigger}")
    else:
        # Run the worker loop
        print("Starting Entourage QueueRuntime...")
        rt.run()
