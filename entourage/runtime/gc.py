"""Bounded retention policy for completed execution graphs."""

import time
from dataclasses import dataclass
from typing import Optional

from .interfaces import GraphStore


@dataclass(frozen=True)
class RetentionPolicy:
    """Keep recent terminal graphs while bounding age and total count."""

    terminal_ttl_seconds: Optional[float] = 7 * 24 * 60 * 60
    max_terminal_sessions: Optional[int] = 1000
    batch_size: int = 100
    interval_seconds: float = 60

    def __post_init__(self):
        if self.terminal_ttl_seconds is not None and self.terminal_ttl_seconds < 0:
            raise ValueError("terminal_ttl_seconds must be >= 0 or None")
        if self.max_terminal_sessions is not None and self.max_terminal_sessions < 0:
            raise ValueError("max_terminal_sessions must be >= 0 or None")
        if self.batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if self.interval_seconds < 0:
            raise ValueError("interval_seconds must be >= 0")


def collect_terminal_sessions(
    store: GraphStore, policy: RetentionPolicy, now: Optional[float] = None
) -> list[str]:
    """Delete one bounded batch selected by TTL and count, oldest first."""
    current_time = time.time() if now is None else now
    sessions = store.get_terminal_sessions()
    overflow = (
        max(0, len(sessions) - policy.max_terminal_sessions)
        if policy.max_terminal_sessions is not None
        else 0
    )
    cutoff = (
        current_time - policy.terminal_ttl_seconds
        if policy.terminal_ttl_seconds is not None
        else None
    )
    selected = []
    for index, session in enumerate(sessions):
        expired = (
            cutoff is not None
            and session.get("completed_at") is not None
            and session["completed_at"] <= cutoff
        )
        if index < overflow or expired:
            selected.append(session["id"])
        if len(selected) == policy.batch_size:
            break

    deleted = []
    for session_id in selected:
        if store.delete_terminal_session(session_id):
            deleted.append(session_id)
    return deleted
