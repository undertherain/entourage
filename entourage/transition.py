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
- ``acknowledge``, ``publish``, ``arm``, and ``disarm`` are *effects*:
  they are recorded inside the same commit, applied to the mailboxes and
  monitor store immediately after it, and cleared once applied. A crash
  between commit and application is repaired by replay at recovery —
  acknowledgements use force semantics (the commit, not the lease, is the
  proof of incorporation), publications carry idempotency ``event_id``s,
  and monitor arming is arm-if-absent — so replay is
  exactly-once-effective.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from .monitors import Monitor


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


@dataclass(frozen=True)
class Spawn:
    """A child session to create atomically with this transition.

    Spawn is an authoritative lifecycle act, exactly-once via the commit:
    the child's session id derives deterministically from the committing
    execution and ``slot``, so crash replay re-spawns idempotently instead
    of twinning the child. Lineage (parent session/execution, slot,
    correlation, notify address) is injected into the child's initial
    state under the ``"spawn"`` key.

    The engine fulfils the plane's child contract mechanically: when the
    child session completes, its final state is published as a correlated
    ``kind: result`` event to ``notify``; when it fails, a ``kind: system``
    death notice goes there instead. Both carry the ``correlation_id`` so
    armed monitors observe them, and both wake a parked parent.

    - ``plan``: what the child runs (any plan form).
    - ``initial_state``: the child's starting state (lineage is merged in).
    - ``slot``: disambiguates multiple spawns in one transition; defaults
      to the list index.
    - ``correlation_id``: defaults to the child session id. A parent that
      wants to block-join chooses its own and parks on
      ``corr:{correlation_id}`` (the derived await conversation).
    - ``notify``: conversation for the child's terminal events; defaults
      to ``corr:{correlation_id}`` (narrow join). A supervisor sets its
      own inbox conversation instead.
    """

    plan: Any
    initial_state: Dict[str, Any] = field(default_factory=dict)
    slot: Optional[str] = None
    correlation_id: Optional[str] = None
    notify: Optional[str] = None


@dataclass
class Transition:
    """Everything a node proposes for one turn; the runtime commits it."""

    state: Dict[str, Any]
    plan: Any = None
    acknowledge: List[Acknowledgement] = field(default_factory=list)
    publish: List[Publication] = field(default_factory=list)
    # Monitors to arm with this turn's dispatches (the expectation rides
    # the same commit as the outbound publish, closing the window where a
    # child dies before anyone expects anything of it) and monitor ids to
    # disarm (e.g. a supervisor incorporating a child's terminal result).
    arm: List[Monitor] = field(default_factory=list)
    disarm: List[str] = field(default_factory=list)
    # Child sessions to create with this commit. Not an outbox effect —
    # the child graph is written structurally inside the same commit that
    # completes this node, like a returned plan is.
    spawn: List[Spawn] = field(default_factory=list)

    @property
    def has_effects(self) -> bool:
        return bool(self.acknowledge or self.publish or self.arm or self.disarm)


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
