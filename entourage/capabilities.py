"""Composable agent capabilities.

A capability is a self-contained unit of agent behaviour — durable facts,
topic tracking, a tool family — that an application *composes into* an agent
rather than obtains by subclassing one. It exists so that adding behaviour is
registration, not inheritance, and so that the units are portable between
agents that share nothing else.

Capabilities come in two kinds, and the distinction is the point:

- **Contributive** (:class:`Capability`) — many per agent. Prompt sections and
  tools are merged in registration order. One capability's contribution cannot
  invalidate another's, so composition is safe by construction and order is a
  presentation choice.
- **Exclusive** (:class:`ConversationLifecycle`) — a capability that *also*
  owns conversation history; at most one per agent. It decides when a segment
  is archived, reset, evicted, or projected for the model. Two owners would
  quietly fight over the same state and the symptom would surface far away, as
  context that drifts or duplicates. Registering a second one is therefore a
  configuration error raised at construction, not a defect discovered later in
  a transcript.
"""

from dataclasses import dataclass, field
from typing import Any, Iterable, List, Optional


@dataclass(frozen=True)
class Turn:
    """The turn being assembled, as a capability may inspect it.

    ``segment`` is the live dialogue so far, system message excluded. ``events``
    carries the originating mailbox events when the caller has them, so a
    capability can distinguish a person typing from a scheduled or system wake
    without the driver having to flatten that away first.
    """

    conversation_id: str
    incoming: str
    segment: List[dict] = field(default_factory=list)
    events: List[dict] = field(default_factory=list)
    kind: str = "user"


@dataclass(frozen=True)
class TurnPlan:
    """What the exclusive lifecycle owner decided for one turn.

    ``history`` replaces the durable segment — a reset, an archive, an
    eviction. ``view`` is what the model sees for *this call only*, leaving the
    stored record untouched; it is how a capability shrinks or reshapes the
    prompt without losing the conversation. ``None`` on either side means
    "leave that side alone", and the common case leaves both alone.

    ``handled`` short-circuits the turn: the lifecycle answered it itself — a
    reset command, a rejected input — and no model call is made.
    """

    history: Optional[List[dict]] = None
    view: Optional[List[dict]] = None
    handled: bool = False
    reply: str = ""


class Capability:
    """Contributive behaviour. Many per agent; every hook is optional.

    Subclass and override only what applies. A capability must not mutate
    conversation history — that is the lifecycle owner's job, and doing it here
    is precisely the conflict the split exists to prevent.
    """

    id = "capability"

    def tools(self) -> List[Any]:
        """Tools this capability adds to the agent."""
        return []

    def prompt_section(self, turn: Turn) -> Optional[str]:
        """A block appended to the system prompt, or ``None`` to contribute nothing."""
        return None

    def after_turn(self, turn: Turn, reply: str) -> None:
        """Observe a completed turn, for durable state a capability keeps itself."""


class ConversationLifecycle(Capability):
    """A capability that also claims conversation history. At most one per agent.

    It is a :class:`Capability` first — it may contribute a prompt section and
    tools like any other — plus the one exclusive hook. Inheriting rather than
    standing beside means the split describes *what a hook may do*, not which
    objects a driver has to special-case.
    """

    id = "lifecycle"

    def before_turn(self, turn: Turn) -> TurnPlan:
        """Decide history and model view for this turn."""
        return TurnPlan()


class CapabilityRegistry:
    """Composes capabilities and enforces the contributive/exclusive split."""

    def __init__(self, capabilities: Iterable[Any] = ()):
        self.capabilities = list(capabilities)

        seen: dict = {}
        for capability in self.capabilities:
            identifier = getattr(capability, "id", None)
            if not identifier:
                raise ValueError(f"{capability!r} has no capability id")
            if identifier in seen:
                raise ValueError(f"duplicate capability id {identifier!r}")
            seen[identifier] = capability

        owners = [
            capability
            for capability in self.capabilities
            if isinstance(capability, ConversationLifecycle)
        ]
        if len(owners) > 1:
            claimed = ", ".join(owner.id for owner in owners)
            raise ValueError(
                "conversation lifecycle is exclusive, but it is claimed by "
                f"{claimed}; compose at most one history owner per agent"
            )
        self.lifecycle = owners[0] if owners else None

    def __iter__(self):
        return iter(self.capabilities)

    def __len__(self) -> int:
        return len(self.capabilities)

    def get(self, capability_id: str) -> Any:
        for capability in self.capabilities:
            if capability.id == capability_id:
                return capability
        raise KeyError(capability_id)

    def tools(self) -> List[Any]:
        return [tool for capability in self.capabilities for tool in capability.tools()]

    def prompt_sections(self, turn: Turn) -> List[str]:
        sections = []
        for capability in self.capabilities:
            section = capability.prompt_section(turn)
            if section and section.strip():
                sections.append(section.strip())
        return sections

    def before_turn(self, turn: Turn) -> TurnPlan:
        if self.lifecycle is None:
            return TurnPlan()
        return self.lifecycle.before_turn(turn)

    def after_turn(self, turn: Turn, reply: str) -> None:
        for capability in self.capabilities:
            capability.after_turn(turn, reply)
