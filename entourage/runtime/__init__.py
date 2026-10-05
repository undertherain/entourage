"""
Entourage runtime — one engine (QueueRuntime), pluggable backends.

Interfaces: GraphStore (execution graph persistence) and ReadyQueue
(transport for ready-work pointers). Backends here: in-memory (local/debug),
SQLite (durable single-box), SQS (import from entourage.runtime.sqs — kept
out of this namespace so boto3 stays an optional dependency).

Retired graph runtime (2026-10-05): kept importable for existing consumers, not developed further. See docs/mailbox-first-scheduling.md.
"""

import warnings as _warnings

_warnings.warn("entourage.runtime: Retired graph runtime (2026-10-05): kept importable for existing consumers, not developed further. See docs/mailbox-first-scheduling.md.", DeprecationWarning, stacklevel=2)

from .interfaces import GraphStore, QueueMessage, ReadyQueue
from .planner import END, HEAD, MERGE, expand_plan
from .memory import InMemoryGraphStore, InMemoryReadyQueue
from .redis_queue import RedisReadyQueue
from .redis_store import RedisGraphStore
from .store import DEFAULT_DB_PATH, SQLiteGraphStore
from .queue import NodeTimeoutError, QueueRuntime
from .local import Runtime
from .client import TriggerClient
from .gc import RetentionPolicy, collect_terminal_sessions
from ..redis_mailbox import RedisMailbox

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
    "NodeTimeoutError",
    "QueueRuntime",
    "Runtime",
    "TriggerClient",
    "RetentionPolicy",
    "collect_terminal_sessions",
    "RedisMailbox",
]
