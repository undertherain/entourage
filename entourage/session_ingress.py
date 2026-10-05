"""Keying policy: which durable session an inbound event belongs to.

Session lifetime is not a runtime property. It follows from two application
decisions: keying (which session an event reaches, decided here or by a parent
that spawns children) and completion (decided by the handler's proposal). The
runtime only owns leases, residency and retention. No transport is imported.
"""

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Iterable, Literal, Optional

from .session_backend import SessionAlreadyExists, SessionBackend


Keying = Literal["event", "conversation", "singleton"]
KEYINGS = ("event", "conversation", "singleton")


@dataclass(frozen=True)
class Member:
    """One deployment alias: a definition version plus how its sessions are keyed.

    event        one session per inbound event, `<alias>:<event_id>`; complete after
                 one activation for triage-style work that should run in parallel.
    conversation one session per external conversation, `<alias>:<conversation>`;
                 serial per conversation, parallel across conversations.
    singleton    one fixed session, `session` or the alias; strictly serial.

    initial_state is copied into a session on first delivery only; an existing
    session is never reset. The executable must be registered by the shard.
    """

    alias: str
    executable: str
    keying: Keying
    initial_state: dict = field(default_factory=dict)
    session: Optional[str] = None

    def __post_init__(self):
        if not isinstance(self.alias, str) or not self.alias or ":" in self.alias:
            raise ValueError("alias must be a nonempty string without ':'")
        if not isinstance(self.executable, str) or not self.executable:
            raise ValueError("executable must be a nonempty definition version")
        if self.keying not in KEYINGS:
            raise ValueError(f"keying must be one of {KEYINGS}")
        if not isinstance(self.initial_state, dict):
            raise ValueError("initial_state must be a JSON object")
        if self.session is not None and self.keying != "singleton":
            raise ValueError("an explicit session ID applies to singleton keying only")
        if self.session is not None and (not isinstance(self.session, str) or not self.session):
            raise ValueError("session must be a nonempty string")


def ensure_session(store: SessionBackend, session_id: str, definition: str,
                   contract: dict, state: dict) -> bool:
    """Create an explicitly addressed session unless it exists; return created.

    For producers that name the session themselves rather than deriving it from a
    keying policy. The definition is bound first, so a producer may create a
    session before any worker for that definition has registered.
    """
    try:
        store.inspect(session_id)
        return False
    except KeyError:
        pass
    store.bind_definition(definition, contract)
    try:
        store.create(session_id, definition, deepcopy(state))
        return True
    except SessionAlreadyExists:
        return False


@dataclass(frozen=True)
class Delivery:
    session_id: str
    created: bool
    appended: bool


class SessionIngress:
    """Resolve an event to a session, create it if absent, append idempotently.

    Creation and append are two backend operations. A crash between them leaves
    a ready session without mail, which runs its initial activation with an
    empty batch; the retried delivery then appends to it. Concurrent deliveries
    for a new key race on creation; one wins, both append. Appending to a
    complete session raises ValueError: under event keying that is a replay of a
    finished event with a new ID, under conversation or singleton keying the
    handler chose to close that key. Purging a completed session forgets it, so a
    later delivery with the same key starts a fresh session.
    """

    def __init__(self, store: SessionBackend, members: Iterable[Member]):
        self.store = store
        self._members = {}
        for member in members:
            if member.alias in self._members:
                raise ValueError(f"duplicate alias {member.alias!r}")
            self._members[member.alias] = member

    def member(self, alias: str) -> Member:
        return self._members[alias]

    def route(self, alias: str, event: dict, *, conversation: Optional[str] = None) -> str:
        """Derive the session ID without touching storage."""
        member = self._members[alias]
        event_id = event.get("event_id")
        if not isinstance(event_id, str) or not event_id:
            raise ValueError("event requires a nonempty string event_id")
        if member.keying == "event":
            return f"{alias}:{event_id}"
        if member.keying == "conversation":
            if not isinstance(conversation, str) or not conversation:
                raise ValueError(f"member {alias!r} requires a conversation key")
            return f"{alias}:{conversation}"
        return member.session or alias

    def ensure(self, alias: str, *, conversation: Optional[str] = None,
               event: Optional[dict] = None) -> Delivery:
        """Create the session an event would reach, without appending anything.

        For an adapter that hands the event to another session first (a
        per-event triage that forwards to the conversation): the forwarding
        publication needs its destination to exist at commit time.
        """
        member = self._members[alias]
        session_id = self.route(alias, event or {"event_id": "-"}, conversation=conversation)
        try:
            self.store.create(session_id, member.executable, deepcopy(member.initial_state))
            created = True
        except SessionAlreadyExists:
            created = False
        return Delivery(session_id, created, False)

    def deliver(self, alias: str, event: dict, *,
                conversation: Optional[str] = None) -> Delivery:
        ensured = self.ensure(alias, conversation=conversation, event=event)
        appended = self.store.append(ensured.session_id, event)
        return Delivery(ensured.session_id, ensured.created, appended)
