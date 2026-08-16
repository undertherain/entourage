"""The Transition — a node's complete proposed outcome for one turn.

Computation proposes; the runtime commits. A node under Entourage is a
transition function in the state-machine sense: it maps (committed state,
drained events) to the next state plus outputs — a Mealy machine over the
durable session. Its return value is therefore called the *transition*: the
full description of everything that changes between two committed states.

Plain returns stay valid sugar — ``state`` or ``(state, plan)`` normalize to
a Transition with empty effects — so nodes that just compute never touch
this module. Only coordination-aware nodes (draining a mailbox, publishing
findings to another agent) construct a Transition explicitly.

Commit and delivery semantics (the transactional-outbox pattern):

- ``state`` and ``plan`` commit atomically with the node's completion
  (``GraphStore.commit_transition``, stage 1).
- ``acknowledge`` and ``publish`` are *effects*: they are recorded inside
  the same commit, applied to the mailboxes immediately after it, and
  cleared once applied. A crash between commit and application is repaired
  by replay at recovery — acknowledgements use force semantics (the commit,
  not the lease, is the proof of incorporation) and publications carry
  idempotency ``event_id``s, so replay is exactly-once-effective.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass(frozen=True)
class Acknowledgement:
    """Mailbox events this turn durably incorporated into the session."""

    conversation_id: str
    event_ids: Tuple[str, ...]

    def __post_init__(self):
        object.__setattr__(self, "event_ids", tuple(self.event_ids))


@dataclass(frozen=True)
class Publication:
    """An outbox entry: an event for another mailbox, delivered post-commit.

    ``target`` is an opaque mailbox address mapped by the runtime's
    injectable resolver — never a hostname or a backend detail.
    ``event_id`` is the idempotency key; when omitted, the runtime derives a
    deterministic one from the committing execution, so replay after a crash
    cannot double-deliver.
    """

    target: str
    conversation: str
    event: Dict[str, Any]
    event_id: Optional[str] = None


@dataclass
class Transition:
    """Everything a node proposes for one turn; the runtime commits it."""

    state: Dict[str, Any]
    plan: Any = None
    acknowledge: List[Acknowledgement] = field(default_factory=list)
    publish: List[Publication] = field(default_factory=list)

    @property
    def has_effects(self) -> bool:
        return bool(self.acknowledge or self.publish)


def normalize_result(result: Any) -> Transition:
    """Compile any supported node return into the canonical Transition.

    Accepted forms: a ``Transition``, the classic ``(state, plan)`` pair,
    or a bare state dict.
    """
    if isinstance(result, Transition):
        return result
    if isinstance(result, tuple) and len(result) == 2:
        state, plan = result
        return Transition(state=state, plan=plan)
    return Transition(state=result)
