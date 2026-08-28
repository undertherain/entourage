"""Transport-neutral return ingress — one receiving mechanism, routed.

External completions and messages reach a graph the same way regardless of
transport: an adapter (Astral subscription, HTTP webhook, Redis consumer,
poller, local callback) normalizes what it received into an
``InboundEvent`` and hands it to the ``IngressRouter``. The router decides
the destination *conversation* — demux happens at ingress, not at drain —
and terminates the event as one idempotent mailbox append. Everything
downstream (wake, claim, acknowledgement riding the transition commit) is
the existing mailbox/graph machinery; the adapter never learns whether the
event resumed a parked execution or joined a resident agent's inbox.

Resolution order for an accepted event:

1. an explicitly registered ``CompletionRoute`` for its correlation id
   (the detach case: the dispatching turn recorded where the later result
   should be delivered);
2. the derived await conversation ``corr:{correlation_id}`` — but only
   while some parked execution actually waits on it, so a late result
   whose wait already timed out falls through instead of stranding;
3. the transport's own conversation hint on the event;
4. the router's default (resident inbox) conversation.

Reliability boundary: ``accept`` returns only after the append is durable
in the mailbox (which deduplicates by ``event_id``), so a transport may
acknowledge its delivery exactly then. Replays of the same event id are
absorbed, not redelivered.

Dispatch-side ordering (the fast-response race): allocate the correlation
id and register the route *before* the transport publishes the request —
route registration is idempotent and an orphaned route (crash before
publish) merely expires. Riding the route registration on the Transition
commit itself is a planned refinement (see docs/coordination-plane.md).
"""

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Protocol, Set


def correlation_conversation(correlation_id: str) -> str:
    """The derived conversation an awaited call parks on.

    Not a per-call mailbox: it is a conversation key inside the one
    journal, created and retired by ordinary append/ack/retention.
    """
    return f"corr:{correlation_id}"


@dataclass(frozen=True)
class InboundEvent:
    """What every transport adapter reduces its delivery to."""

    event_id: str
    kind: str
    source: str
    payload: Dict[str, Any] = field(default_factory=dict)
    correlation_id: Optional[str] = None
    conversation_id: Optional[str] = None
    created_at: Optional[float] = None


class OutboundTransport(Protocol):
    """Publishes a request/command; concrete reply addresses stay inside."""

    def publish(self, message: Dict[str, Any]) -> None: ...


class InboundTransport(Protocol):
    """Delivers external events by calling the router's ``accept``.

    An adapter owns its listening lifecycle, source verification, and
    translation into ``InboundEvent``; it acknowledges its transport only
    after ``accept`` returns.
    """

    def receive(self, handler: Callable[[InboundEvent], str]) -> None: ...


class RouteStore(ABC):
    """Durable correlation → destination-conversation registrations."""

    @abstractmethod
    def register(
        self,
        correlation_id: str,
        conversation_id: str,
        expires_at: Optional[float] = None,
    ) -> None:
        """Record (idempotently re-record) where a correlated result goes."""

    @abstractmethod
    def resolve(self, correlation_id: str) -> Optional[str]:
        """The registered conversation, or None if absent or expired."""


class InMemoryRouteStore(RouteStore):
    """Reference backend; durable stores can live beside the graph store."""

    def __init__(self):
        self._routes: Dict[str, Dict[str, Any]] = {}

    def register(
        self,
        correlation_id: str,
        conversation_id: str,
        expires_at: Optional[float] = None,
    ) -> None:
        self._routes[correlation_id] = {
            "conversation_id": conversation_id,
            "expires_at": expires_at,
        }

    def resolve(self, correlation_id: str) -> Optional[str]:
        route = self._routes.get(correlation_id)
        if route is None:
            return None
        expires_at = route["expires_at"]
        if expires_at is not None and time.time() >= expires_at:
            # Staleness belongs to the route: a lapsed registration stops
            # routing but is left for inspection until re-registered.
            return None
        return route["conversation_id"]


class IngressRouter:
    """Normalized, durable acceptance of external events into mailboxes.

    ``waiting_conversations`` is the runtime's live view of parked waits
    (``QueueRuntime.waiting_conversations``); ``wake`` is called after a
    successful append so a parked execution becomes runnable without
    waiting for the engine's next tick (``QueueRuntime.wake_due_waits``);
    ``observe`` feeds armed monitors — a correlated result satisfies a
    deadline expectation, any event refreshes its source's heartbeat
    (``QueueRuntime.observe_monitors``). All are optional: without them the
    router still delivers, and the engine's poll-cadence tick picks the
    wake up.
    """

    def __init__(
        self,
        mailbox,
        routes: Optional[RouteStore] = None,
        default_conversation: Optional[str] = None,
        waiting_conversations: Optional[Callable[[], Set[str]]] = None,
        wake: Optional[Callable[[], int]] = None,
        observe: Optional[Callable[..., int]] = None,
    ):
        self.mailbox = mailbox
        self.routes = routes or InMemoryRouteStore()
        self.default_conversation = default_conversation
        self.waiting_conversations = waiting_conversations
        self.wake = wake
        self.observe = observe

    def register(
        self,
        correlation_id: str,
        conversation_id: str,
        expires_at: Optional[float] = None,
    ) -> None:
        self.routes.register(correlation_id, conversation_id, expires_at)

    def _resolve(self, event: InboundEvent) -> Optional[str]:
        if event.correlation_id:
            routed = self.routes.resolve(event.correlation_id)
            if routed:
                return routed
            derived = correlation_conversation(event.correlation_id)
            if (
                self.waiting_conversations is not None
                and derived in self.waiting_conversations()
            ):
                return derived
        return event.conversation_id or self.default_conversation

    def accept(self, event: InboundEvent) -> str:
        """Deliver an inbound event; return the conversation it joined.

        Durable before acknowledged: when this returns, the event is
        appended (or was already known — append deduplicates by
        ``event_id``) and the transport may ack. Raises ``ValueError``
        when no destination resolves, which the adapter should surface as
        a delivery failure rather than ack away.
        """
        conversation = self._resolve(event)
        if not conversation:
            raise ValueError(
                f"no destination for inbound event {event.event_id!r} "
                f"(correlation {event.correlation_id!r}, no default inbox)"
            )
        envelope = {
            "event_id": event.event_id,
            "kind": event.kind,
            "source": event.source,
            "payload": event.payload,
        }
        if event.correlation_id:
            envelope["correlation_id"] = event.correlation_id
        if event.created_at is not None:
            envelope["created_at"] = event.created_at
        self.mailbox.append(conversation, envelope)
        if self.observe is not None:
            self.observe(correlation_id=event.correlation_id, source=event.source)
        if self.wake is not None:
            self.wake()
        return conversation
