"""Ingress-only client for services that trigger an Entourage runtime."""

import time
from typing import Any, Dict

from .interfaces import ReadyQueue


class TriggerClient:
    """Publish workflow triggers without constructing a runtime worker."""

    def __init__(self, queue: ReadyQueue):
        self.queue = queue

    def send_trigger(
        self, trigger: str, state: Dict[str, Any], serial_key: str = None
    ):
        payload = {
            "type": "trigger",
            "trigger": trigger,
            "state": state,
            "time_created": time.time(),
        }
        if serial_key is not None:
            payload["serial_key"] = serial_key
        self.queue.send(payload)
