"""Deployment configuration shared by Entourage applications and adapters."""

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class RedisRuntimeConfig:
    """One agent's namespaces on a shared Redis server.

    Redis is shared infrastructure; namespaces isolate schedulers whose workers
    do not necessarily register the same nodes.
    """

    url: str = "redis://localhost:6379/0"
    prefix: str = "entourage"

    @property
    def graph_namespace(self) -> str:
        return f"{self.prefix}:graph"

    @property
    def queue_namespace(self) -> str:
        return f"{self.prefix}:queue"

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "RedisRuntimeConfig":
        return cls(url=str(value.get("url", cls.url)), prefix=str(value.get("prefix", cls.prefix)))

    def graph_store(self):
        from .runtime import RedisGraphStore

        return RedisGraphStore(url=self.url, namespace=self.graph_namespace)

    def ready_queue(self):
        from .runtime import RedisReadyQueue

        return RedisReadyQueue(url=self.url, namespace=self.queue_namespace)
