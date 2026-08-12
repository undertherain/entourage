"""Minimal Telegram Bot API adapter.

The adapter only translates channel messages.  It deliberately owns neither
conversation history nor agent behavior: an incoming update is handed to a
callback, and an outgoing call returns Telegram's sent Message object.
"""

import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)
DEFAULT_CONFIG_PATH = Path("config/telegram.yaml")


class TelegramError(RuntimeError):
    """A Telegram Bot API request failed."""


def load_config(path: Optional[Path] = None) -> Dict[str, Any]:
    """Load environment config, optionally overlaid by a legacy YAML file."""
    config: Dict[str, Any] = {}
    if path is not None and Path(path).exists():
        try:
            import yaml
        except ImportError as exc:
            raise RuntimeError("PyYAML is required to read Telegram YAML config") from exc
        with Path(path).open(encoding="utf-8") as stream:
            config.update(yaml.safe_load(stream) or {})

    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if token:
        config["bot_token"] = token
    allowed = os.environ.get("TELEGRAM_ALLOWED_CHAT_IDS")
    if allowed is not None:
        config["allowed_chat_ids"] = [item.strip() for item in allowed.split(",") if item.strip()]
    return config


def _request_json(url: str, payload: Optional[Dict[str, Any]] = None, timeout: float = 35) -> Dict:
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    try:
        with urlopen(Request(url, data=data, headers=headers), timeout=timeout) as response:
            result = json.loads(response.read().decode("utf-8"))
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise TelegramError(f"Telegram request failed: {exc}") from exc
    if not result.get("ok"):
        raise TelegramError(result.get("description", "Telegram returned ok=false"))
    return result


class TelegramListener:
    """Long-poll Telegram and pass normalized text messages to a callback."""

    def __init__(
        self,
        on_message: Callable[[Dict[str, Any]], None],
        bot_token: Optional[str] = None,
        allowed_chat_ids: Optional[Set[str]] = None,
        config_path: Optional[Path] = None,
    ):
        config = load_config(config_path)
        self.bot_token = bot_token or config.get("bot_token")
        if not self.bot_token:
            raise ValueError("TELEGRAM_BOT_TOKEN is not set")
        configured_ids = config.get("allowed_chat_ids", [])
        self.allowed_chat_ids = (
            {str(item) for item in allowed_chat_ids}
            if allowed_chat_ids is not None
            else {str(item) for item in configured_ids}
        )
        self.on_message = on_message
        self.api_base = f"https://api.telegram.org/bot{self.bot_token}"
        self.next_update_id: Optional[int] = None

    def _get_updates(self) -> List[Dict]:
        params: Dict[str, Any] = {
            "timeout": 30,
            "allowed_updates": json.dumps(["message"]),
        }
        if self.next_update_id is not None:
            params["offset"] = self.next_update_id
        return _request_json(
            f"{self.api_base}/getUpdates?{urlencode(params)}", timeout=35
        ).get("result", [])

    def _process_update(self, update: Dict):
        message = update.get("message") or {}
        text = message.get("text")
        chat_id = str((message.get("chat") or {}).get("id", ""))
        if not text or not chat_id:
            return
        if not self.allowed_chat_ids or chat_id not in self.allowed_chat_ids:
            logger.warning("Ignoring Telegram message from non-allowlisted chat %s", chat_id)
            return

        sender_data = message.get("from") or {}
        sender = sender_data.get("username") or sender_data.get("first_name") or "unknown"
        self.on_message({
            "chat_id": chat_id,
            "sender": sender,
            "sender_id": str(sender_data.get("id", "")),
            "text": text,
            "message_id": message["message_id"],
            "update_id": update["update_id"],
            "timestamp": message.get("date"),
        })

    def run(self, retry_delay: float = 5.0):
        logger.info("Telegram listener started")
        while True:
            try:
                updates = self._get_updates()
                for update in updates:
                    # Advance only after the callback has durably queued the trigger.
                    self._process_update(update)
                    self.next_update_id = update["update_id"] + 1
            except Exception as exc:  # callback failures must leave the update unconfirmed
                logger.error("%s; retrying in %.1fs", exc, retry_delay)
                time.sleep(retry_delay)


class TelegramSender:
    """Send messages directly through the Telegram Bot API."""

    def __init__(self, bot_token: Optional[str] = None, config_path: Optional[Path] = None):
        config = load_config(config_path)
        self.bot_token = bot_token or config.get("bot_token")
        if not self.bot_token:
            raise ValueError("TELEGRAM_BOT_TOKEN is not set")
        self.api_base = f"https://api.telegram.org/bot{self.bot_token}"

    def send(self, chat_id: str, text: str) -> Dict:
        return _request_json(
            f"{self.api_base}/sendMessage",
            {"chat_id": chat_id, "text": text},
            timeout=15,
        )
