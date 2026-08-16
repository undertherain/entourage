"""Durable-semantics mailboxes for events that join a running conversation."""

import copy
import threading
import time
import uuid
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class Mailbox(ABC):
    """Append/claim/ack contract for per-conversation event inboxes.

    Claims are leases. A consumer must acknowledge incorporated events; an
    expired lease makes them claimable again after a worker crash. Backends
    must make append idempotent by ``event_id``.
    """

    @abstractmethod
    def append(self, conversation_id: str, event: Dict[str, Any]) -> str:
        """Append an event, returning its stable id (duplicates are ignored)."""

    @abstractmethod
    def claim(
        self,
        conversation_id: str,
        consumer: str,
        limit: int = 20,
        lease_seconds: float = 30,
    ) -> List[Dict[str, Any]]:
        """Lease pending events in append order."""

    @abstractmethod
    def claim_any(
        self, consumer: str, limit: int = 20, lease_seconds: float = 30
    ) -> List[Dict[str, Any]]:
        """Lease the oldest ready conversation's events, without mixing conversations."""

    @abstractmethod
    def acknowledge(
        self,
        conversation_id: str,
        consumer: str,
        event_ids: List[str],
        force: bool = False,
    ) -> None:
        """Mark events leased by this consumer as durably incorporated.

        With ``force=True`` the lease check is skipped and unknown or
        already-acknowledged events are ignored. This exists for replaying a
        committed transition's acknowledgements after a crash: by then the
        commit — not the lease, which may have expired — is the proof of
        incorporation, and replay must be idempotent.
        """

    @abstractmethod
    def release(
        self, conversation_id: str, consumer: str, event_ids: List[str]
    ) -> None:
        """Return leased events to pending without acknowledging them."""

    @abstractmethod
    def purge_acknowledged(
        self,
        conversation_id: Optional[str] = None,
        older_than: Optional[float] = None,
        limit: int = 100,
    ) -> int:
        """Remove acknowledged events in bounded append order."""

    @abstractmethod
    def purge_deduplication_keys(self, older_than: float, limit: int = 100) -> int:
        """Remove old idempotency tombstones whose event payload is already gone."""


class InMemoryMailbox(Mailbox):
    """Thread-safe reference backend with lease and idempotence semantics."""

    def __init__(self):
        self._events: Dict[str, List[Dict[str, Any]]] = {}
        self._event_ids: Dict[str, tuple[str, float]] = {}
        self._next_sequence = 0
        self._condition = threading.Condition()

    def append(self, conversation_id: str, event: Dict[str, Any]) -> str:
        if not conversation_id:
            raise ValueError("conversation_id is required")
        value = copy.deepcopy(event)
        event_id = str(value.get("event_id") or uuid.uuid4().hex)
        with self._condition:
            known_conversation = self._event_ids.get(event_id)
            if known_conversation is not None:
                if known_conversation[0] != conversation_id:
                    raise ValueError(f"event_id {event_id!r} belongs to another conversation")
                return event_id
            value.update(
                {
                    "event_id": event_id,
                    "conversation_id": conversation_id,
                    "created_at": value.get("created_at", time.time()),
                    "status": "pending",
                    "consumer": None,
                    "lease_until": None,
                    "acknowledged_at": None,
                    "_sequence": self._next_sequence,
                }
            )
            self._next_sequence += 1
            self._events.setdefault(conversation_id, []).append(value)
            self._event_ids[event_id] = (conversation_id, time.time())
            self._condition.notify_all()
        return event_id

    def claim(
        self,
        conversation_id: str,
        consumer: str,
        limit: int = 20,
        lease_seconds: float = 30,
    ) -> List[Dict[str, Any]]:
        if not consumer:
            raise ValueError("consumer is required")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if lease_seconds < 0:
            raise ValueError("lease_seconds must be >= 0")
        now = time.time()
        claimed = []
        with self._condition:
            for event in self._events.get(conversation_id, []):
                expired = (
                    event["status"] == "leased"
                    and event["lease_until"] is not None
                    and event["lease_until"] <= now
                )
                if event["status"] != "pending" and not expired:
                    continue
                event["status"] = "leased"
                event["consumer"] = consumer
                event["lease_until"] = now + lease_seconds
                claimed.append(self._public(event))
                if len(claimed) == limit:
                    break
        return claimed

    def claim_any(
        self, consumer: str, limit: int = 20, lease_seconds: float = 30
    ) -> List[Dict[str, Any]]:
        with self._condition:
            now = time.time()
            oldest = None
            for conversation_id, events in self._events.items():
                for event in events:
                    claimable = event["status"] == "pending" or (
                        event["status"] == "leased"
                        and event["lease_until"] is not None
                        and event["lease_until"] <= now
                    )
                    if claimable and (
                        oldest is None or event["_sequence"] < oldest[0]
                    ):
                        oldest = (event["_sequence"], conversation_id)
                    if claimable:
                        break
            if oldest is None:
                return []
            conversation_id = oldest[1]
        return self.claim(conversation_id, consumer, limit, lease_seconds)

    def acknowledge(
        self,
        conversation_id: str,
        consumer: str,
        event_ids: List[str],
        force: bool = False,
    ) -> None:
        with self._condition:
            if force:
                wanted = set(event_ids)
                events = [
                    event
                    for event in self._events.get(conversation_id, [])
                    if event["event_id"] in wanted
                    and event["status"] != "acknowledged"
                ]
            else:
                events = self._selected(conversation_id, event_ids)
            for event in events:
                if not force:
                    self._require_lease(event, consumer)
                event["status"] = "acknowledged"
                event["acknowledged_at"] = time.time()
                event["consumer"] = None
                event["lease_until"] = None

    def release(
        self, conversation_id: str, consumer: str, event_ids: List[str]
    ) -> None:
        with self._condition:
            for event in self._selected(conversation_id, event_ids):
                self._require_lease(event, consumer)
                event["status"] = "pending"
                event["consumer"] = None
                event["lease_until"] = None
            self._condition.notify_all()

    def purge_acknowledged(
        self,
        conversation_id: Optional[str] = None,
        older_than: Optional[float] = None,
        limit: int = 100,
    ) -> int:
        if limit < 1:
            raise ValueError("limit must be >= 1")
        removed = 0
        with self._condition:
            conversation_ids = (
                [conversation_id]
                if conversation_id is not None
                else list(self._events)
            )
            candidates = sorted(
                (
                    event
                    for current in conversation_ids
                    for event in self._events.get(current, [])
                    if event["status"] == "acknowledged"
                    and (
                        older_than is None
                        or (event["acknowledged_at"] or float("inf")) <= older_than
                    )
                ),
                key=lambda event: event["_sequence"],
            )[:limit]
            remove_ids = {event["event_id"] for event in candidates}
            for current in conversation_ids:
                kept = []
                for event in self._events.get(current, []):
                    if event["event_id"] in remove_ids:
                        removed += 1
                    else:
                        kept.append(event)
                if kept:
                    self._events[current] = kept
                else:
                    self._events.pop(current, None)
        return removed

    def purge_deduplication_keys(self, older_than: float, limit: int = 100) -> int:
        if limit < 1:
            raise ValueError("limit must be >= 1")
        with self._condition:
            live_ids = {
                event["event_id"]
                for events in self._events.values()
                for event in events
            }
            candidates = [
                event_id
                for event_id, (_conversation_id, seen_at) in self._event_ids.items()
                if event_id not in live_ids and seen_at <= older_than
            ][:limit]
            for event_id in candidates:
                del self._event_ids[event_id]
        return len(candidates)

    def wait_for_events(
        self, conversation_id: Optional[str] = None, timeout: Optional[float] = None
    ) -> bool:
        """Block the local demo until claimable work exists; not worker polling."""
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._condition:
            while not self._has_claimable(conversation_id):
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return False
                lease_delay = self._next_lease_delay(conversation_id)
                waits = [value for value in (remaining, lease_delay) if value is not None]
                self._condition.wait(min(waits) if waits else None)
            return True

    def pending_count(self, conversation_id: str) -> int:
        with self._condition:
            return sum(
                event["status"] == "pending"
                for event in self._events.get(conversation_id, [])
            )

    def _has_claimable(self, conversation_id: Optional[str]) -> bool:
        now = time.time()
        return any(
            event["status"] == "pending"
            or (
                event["status"] == "leased"
                and event["lease_until"] is not None
                and event["lease_until"] <= now
            )
            for event in (
                self._events.get(conversation_id, [])
                if conversation_id is not None
                else [item for events in self._events.values() for item in events]
            )
        )

    def _next_lease_delay(self, conversation_id: Optional[str]) -> Optional[float]:
        now = time.time()
        expiries = [
            event["lease_until"]
            for event in (
                self._events.get(conversation_id, [])
                if conversation_id is not None
                else [item for events in self._events.values() for item in events]
            )
            if event["status"] == "leased" and event["lease_until"] is not None
        ]
        return max(0, min(expiries) - now) if expiries else None

    def _selected(self, conversation_id: str, event_ids: List[str]) -> List[Dict[str, Any]]:
        wanted = set(event_ids)
        found = [
            event
            for event in self._events.get(conversation_id, [])
            if event["event_id"] in wanted
        ]
        if len(found) != len(wanted):
            raise KeyError("one or more mailbox events were not found")
        return found

    @staticmethod
    def _require_lease(event: Dict[str, Any], consumer: str) -> None:
        expired = event["lease_until"] is not None and event["lease_until"] <= time.time()
        if event["status"] != "leased" or event["consumer"] != consumer or expired:
            raise ValueError(f"event {event['event_id']!r} is not leased by {consumer!r}")

    @staticmethod
    def _public(event: Dict[str, Any]) -> Dict[str, Any]:
        result = copy.deepcopy(event)
        result.pop("status", None)
        result.pop("consumer", None)
        result.pop("lease_until", None)
        result.pop("acknowledged_at", None)
        result.pop("_sequence", None)
        return result
