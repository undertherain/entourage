"""
Entourage runtime — one engine (QueueRuntime), pluggable backends.

Interfaces: GraphStore (execution graph persistence) and ReadyQueue
(transport for ready-work pointers). Backends here: in-memory (local/debug),
SQLite (durable single-box), SQS (import from entourage.runtime.sqs — kept
out of this namespace so boto3 stays an optional dependency).
"""

from .interfaces import GraphStore, QueueMessage, ReadyQueue
from .planner import END, HEAD, MERGE, expand_plan
from .memory import InMemoryGraphStore, InMemoryReadyQueue
from .redis_queue import RedisReadyQueue
from .redis_store import RedisGraphStore
from .store import DEFAULT_DB_PATH, SQLiteGraphStore
from .queue import QueueRuntime
from .local import Runtime

__all__ = [
    "GraphStore",
    "ReadyQueue",
    "QueueMessage",
    "HEAD",
    "END",
    "MERGE",
    "expand_plan",
    "InMemoryGraphStore",
    "InMemoryReadyQueue",
    "RedisReadyQueue",
    "RedisGraphStore",
    "SQLiteGraphStore",
    "DEFAULT_DB_PATH",
    "QueueRuntime",
    "Runtime",
]
