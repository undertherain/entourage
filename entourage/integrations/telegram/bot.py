"""
Telegram integration for Entourage.

Polls Telegram Bot API for messages, sends triggers to the queue,
and provides a send_message function for pipeline nodes.

Config: config/telegram.yaml
"""

import json
import logging
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import requests
import yaml

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = Path("config/telegram.yaml")


def load_config(path: Path = DEFAULT_CONFIG_PATH) -> Dict:
    with open(path) as f:
        return yaml.safe_load(f)


class TelegramListener:
    """
    Polls Telegram Bot API and fires a callback for each incoming message.

    Usage:
        listener = TelegramListener(
            config_path="config/telegram.yaml",
            on_message=my_callback,
        )
        listener.run()

    The callback receives a dict:
        {
            "chat_id": str,
            "sender": str,
            "text": str,
            "message_id": int,
            "history": [list of recent messages in this chat],
        }
    """

    def __init__(
        self,
        on_message: Callable[[Dict[str, Any]], None],
        config_path: Path = DEFAULT_CONFIG_PATH,
        history_size: int = 20,
    ):
        self.config = load_config(config_path)
        self.bot_token = self.config["bot_token"]
        self.bot_id = self.config.get("bot_id", self.bot_token.split(":")[0])
        self.on_message = on_message
        self.history_size = history_size

        self.api_base = f"https://api.telegram.org/bot{self.bot_token}"
        self.last_update_id = 0
        self.chat_histories: Dict[str, List[Dict]] = defaultdict(list)

    def _get_updates(self) -> List[Dict]:
        try:
            resp = requests.get(
                f"{self.api_base}/getUpdates",
                params={"offset": self.last_update_id + 1, "timeout": 10},
                timeout=15,
            )
            data = resp.json()
            if data.get("ok"):
                return data.get("result", [])
        except Exception as e:
            logger.error("Error polling Telegram: %s", e)
        return []

    def _process_update(self, update: Dict):
        self.last_update_id = update["update_id"]
        message = update.get("message")
        if not message or "text" not in message:
            return

        chat_id = str(message["chat"]["id"])
        sender = message["from"].get("username") or message["from"].get("first_name", "unknown")
        text = message["text"]
        message_id = message["message_id"]

        # Track history per chat
        entry = {"sender": sender, "text": text, "message_id": message_id}
        # Mark our own messages
        if str(message["from"].get("id")) == str(self.bot_id):
            entry["bot_id"] = self.bot_id

        history = self.chat_histories[chat_id]
        history.append(entry)
        if len(history) > self.history_size:
            self.chat_histories[chat_id] = history[-self.history_size:]

        self.on_message({
            "chat_id": chat_id,
            "sender": sender,
            "text": text,
            "message_id": message_id,
            "history": list(self.chat_histories[chat_id]),
        })

    def run(self, poll_interval: float = 1.0):
        """Poll forever."""
        logger.info("Telegram listener started for bot %s", self.bot_id)
        while True:
            updates = self._get_updates()
            for update in updates:
                self._process_update(update)
            if not updates:
                time.sleep(poll_interval)


class TelegramSender:
    """
    Send messages via Telegram Bot API (direct) or via your Telegram service container.
    """

    def __init__(self, config_path: Path = DEFAULT_CONFIG_PATH):
        self.config = load_config(config_path)
        self.bot_token = self.config["bot_token"]
        self.bot_id = self.config.get("bot_id", self.bot_token.split(":")[0])
        self.service_url = self.config.get("service_url")
        self.service_token = self.config.get("service_token")

    def send(self, chat_id: str, text: str) -> Dict:
        """Send a message. Uses service container if configured, otherwise direct API."""
        if self.service_url:
            return self._send_via_service(chat_id, text)
        return self._send_direct(chat_id, text)

    def _send_direct(self, chat_id: str, text: str) -> Dict:
        resp = requests.post(
            f"https://api.telegram.org/bot{self.bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": text},
        )
        result = resp.json()
        if not result.get("ok"):
            logger.error("Telegram send failed: %s", result)
        return result

    def _send_via_service(self, chat_id: str, text: str) -> Dict:
        headers = {}
        if self.service_token:
            headers["Authorization"] = f"Bearer {self.service_token}"
        resp = requests.post(
            f"{self.service_url}/send_message/{self.bot_id}",
            json={"chat_id": chat_id, "text": text},
            headers=headers,
        )
        return resp.json()
