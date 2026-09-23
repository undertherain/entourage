"""Trusted invocation context for tools called on behalf of an agent turn.

The worker executing a turn sets the context; tools read it instead of accepting
caller identity as model-supplied arguments. The fields are routing references
owned by the application: the runtime neither interprets nor validates them.
"""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class ToolInvocation:
    agent_id: str
    conversation_id: str
    reply_target: Optional[str] = None
    chat_id: Optional[str] = None
    request_id: Optional[str] = None


_CURRENT: ContextVar[Optional[ToolInvocation]] = ContextVar(
    "entourage_tool_invocation", default=None
)


def current_invocation() -> ToolInvocation:
    invocation = _CURRENT.get()
    if invocation is None:
        raise RuntimeError("this tool requires an agent-turn invocation context")
    return invocation


@contextmanager
def invocation_context(invocation: ToolInvocation):
    token = _CURRENT.set(invocation)
    try:
        yield invocation
    finally:
        _CURRENT.reset(token)
