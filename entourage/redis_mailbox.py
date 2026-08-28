"""Redis-backed durable conversation mailbox."""

import hashlib
import json
import time
import uuid
from typing import Any, Dict, List, Optional

from .mailbox import Mailbox


class RedisMailbox(Mailbox):
    """Mailbox strategy backed by one Redis namespace.

    Mutations use a short namespace lock. This favors a small, auditable
    reference implementation; the contract can later move to Lua without
    changing applications.
    """

    def __init__(
        self,
        client=None,
        url: str = "redis://localhost:6379/0",
        namespace: str = "entourage:mailbox",
        poll_interval: float = 0.05,
    ):
        if client is None:
            import redis

            client = redis.Redis.from_url(url, decode_responses=True)
        self._r = client
        self.namespace = namespace
        self.poll_interval = poll_interval

    def _lock(self):
        return self._r.lock(
            f"{self.namespace}:lock", timeout=10, blocking_timeout=10
        )

    def _event_key(self, event_id: str) -> str:
        digest = hashlib.sha256(event_id.encode("utf-8")).hexdigest()
        return f"{self.namespace}:event:{digest}"

    def _events_key(self, conversation_id: str) -> str:
        digest = hashlib.sha256(conversation_id.encode("utf-8")).hexdigest()
        return f"{self.namespace}:events:{digest}"

    def _conversations_key(self) -> str:
        return f"{self.namespace}:conversations"

    def _dedup_key(self) -> str:
        return f"{self.namespace}:dedup"

    def _sequence_key(self) -> str:
        return f"{self.namespace}:sequence"

    def append(self, conversation_id: str, event: Dict[str, Any]) -> str:
        if not conversation_id:
            raise ValueError("conversation_id is required")
        value = dict(event)
        event_id = str(value.get("event_id") or uuid.uuid4().hex)
        with self._lock():
            known = self._r.hget(self._dedup_key(), event_id)
            if known:
                metadata = json.loads(known)
                if metadata["conversation_id"] != conversation_id:
                    raise ValueError(
                        f"event_id {event_id!r} belongs to another conversation"
                    )
                return event_id
            sequence = int(self._r.incr(self._sequence_key()))
            value.update({
                "event_id": event_id,
                "conversation_id": conversation_id,
                "created_at": value.get("created_at", time.time()),
                "status": "pending",
                "consumer": None,
                "lease_until": None,
                "acknowledged_at": None,
                "_sequence": sequence,
            })
            pipe = self._r.pipeline()
            pipe.set(self._event_key(event_id), json.dumps(value, ensure_ascii=False))
            pipe.zadd(self._events_key(conversation_id), {event_id: sequence})
            pipe.hset(
                self._dedup_key(),
                event_id,
                json.dumps({"conversation_id": conversation_id, "seen_at": time.time()}),
            )
            pipe.sadd(self._conversations_key(), conversation_id)
            pipe.execute()
        return event_id

    def _load(self, event_id: str) -> Optional[Dict[str, Any]]:
        value = self._r.get(self._event_key(event_id))
        return json.loads(value) if value else None

    @staticmethod
    def _claimable(event: Dict[str, Any], now: float) -> bool:
        return event["status"] == "pending" or (
            event["status"] == "leased"
            and event.get("lease_until") is not None
            and event["lease_until"] <= now
        )

    def _claim_locked(
        self, conversation_id: str, consumer: str, limit: int, lease_seconds: float
    ) -> List[Dict[str, Any]]:
        now = time.time()
        claimed = []
        for event_id in self._r.zrange(self._events_key(conversation_id), 0, -1):
            event = self._load(event_id)
            if not event or not self._claimable(event, now):
                continue
            event.update({
                "status": "leased",
                "consumer": consumer,
                "lease_until": now + lease_seconds,
            })
            self._r.set(
                self._event_key(event_id), json.dumps(event, ensure_ascii=False)
            )
            claimed.append(self._public(event))
            if len(claimed) == limit:
                break
        return claimed

    def claimable_count(self, conversation_id: str) -> int:
        now = time.time()
        count = 0
        for event_id in self._r.zrange(self._events_key(conversation_id), 0, -1):
            event = self._load(event_id)
            if event and self._claimable(event, now):
                count += 1
        return count

    def claim(
        self,
        conversation_id: str,
        consumer: str,
        limit: int = 20,
        lease_seconds: float = 30,
    ) -> List[Dict[str, Any]]:
        self._validate_claim(consumer, limit, lease_seconds)
        with self._lock():
            return self._claim_locked(conversation_id, consumer, limit, lease_seconds)

    def claim_any(
        self, consumer: str, limit: int = 20, lease_seconds: float = 30
    ) -> List[Dict[str, Any]]:
        self._validate_claim(consumer, limit, lease_seconds)
        with self._lock():
            now = time.time()
            oldest = None
            for conversation_id in self._r.smembers(self._conversations_key()) or set():
                for event_id in self._r.zrange(self._events_key(conversation_id), 0, -1):
                    event = self._load(event_id)
                    if event and self._claimable(event, now):
                        if oldest is None or event["_sequence"] < oldest[0]:
                            oldest = (event["_sequence"], conversation_id)
                        break
            if oldest is None:
                return []
            return self._claim_locked(oldest[1], consumer, limit, lease_seconds)

    @staticmethod
    def _validate_claim(consumer: str, limit: int, lease_seconds: float) -> None:
        if not consumer:
            raise ValueError("consumer is required")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        if lease_seconds < 0:
            raise ValueError("lease_seconds must be >= 0")

    def _selected(self, conversation_id: str, event_ids: List[str]) -> List[Dict[str, Any]]:
        events = [self._load(event_id) for event_id in set(event_ids)]
        if any(event is None for event in events) or any(
            event["conversation_id"] != conversation_id for event in events if event
        ):
            raise KeyError("one or more mailbox events were not found")
        return events

    @staticmethod
    def _require_lease(event: Dict[str, Any], consumer: str) -> None:
        expired = event.get("lease_until") is not None and event["lease_until"] <= time.time()
        if event["status"] != "leased" or event.get("consumer") != consumer or expired:
            raise ValueError(f"event {event['event_id']!r} is not leased by {consumer!r}")

    def acknowledge(
        self,
        conversation_id: str,
        consumer: str,
        event_ids: List[str],
        force: bool = False,
    ) -> None:
        with self._lock():
            if force:
                events = [
                    event
                    for event in (self._load(eid) for eid in set(event_ids))
                    if event
                    and event["conversation_id"] == conversation_id
                    and event["status"] != "acknowledged"
                ]
            else:
                events = self._selected(conversation_id, event_ids)
            for event in events:
                if not force:
                    self._require_lease(event, consumer)
                event.update({
                    "status": "acknowledged",
                    "acknowledged_at": time.time(),
                    "consumer": None,
                    "lease_until": None,
                })
                self._r.set(
                    self._event_key(event["event_id"]),
                    json.dumps(event, ensure_ascii=False),
                )

    def release(
        self, conversation_id: str, consumer: str, event_ids: List[str]
    ) -> None:
        with self._lock():
            for event in self._selected(conversation_id, event_ids):
                self._require_lease(event, consumer)
                event.update({"status": "pending", "consumer": None, "lease_until": None})
                self._r.set(
                    self._event_key(event["event_id"]),
                    json.dumps(event, ensure_ascii=False),
                )

    def purge_acknowledged(
        self,
        conversation_id: Optional[str] = None,
        older_than: Optional[float] = None,
        limit: int = 100,
    ) -> int:
        if limit < 1:
            raise ValueError("limit must be >= 1")
        with self._lock():
            conversation_ids = (
                [conversation_id]
                if conversation_id is not None
                else sorted(self._r.smembers(self._conversations_key()) or set())
            )
            candidates = []
            for current in conversation_ids:
                for event_id in self._r.zrange(self._events_key(current), 0, -1):
                    event = self._load(event_id)
                    if event and event["status"] == "acknowledged" and (
                        older_than is None
                        or (event.get("acknowledged_at") or float("inf")) <= older_than
                    ):
                        candidates.append(event)
            candidates.sort(key=lambda event: event["_sequence"])
            pipe = self._r.pipeline()
            for event in candidates[:limit]:
                current = event["conversation_id"]
                pipe.delete(self._event_key(event["event_id"]))
                pipe.zrem(self._events_key(current), event["event_id"])
            pipe.execute()
            for current in conversation_ids:
                if self._r.zcard(self._events_key(current)) == 0:
                    self._r.srem(self._conversations_key(), current)
            return min(len(candidates), limit)

    def purge_deduplication_keys(self, older_than: float, limit: int = 100) -> int:
        if limit < 1:
            raise ValueError("limit must be >= 1")
        with self._lock():
            candidates = []
            for event_id, raw in (self._r.hgetall(self._dedup_key()) or {}).items():
                metadata = json.loads(raw)
                if metadata["seen_at"] <= older_than and not self._r.exists(
                    self._event_key(event_id)
                ):
                    candidates.append(event_id)
                    if len(candidates) == limit:
                        break
            if candidates:
                self._r.hdel(self._dedup_key(), *candidates)
            return len(candidates)

    def wait_for_events(
        self, conversation_id: Optional[str] = None, timeout: Optional[float] = None
    ) -> bool:
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            with self._lock():
                now = time.time()
                conversations = (
                    [conversation_id]
                    if conversation_id is not None
                    else self._r.smembers(self._conversations_key()) or set()
                )
                for current in conversations:
                    for event_id in self._r.zrange(self._events_key(current), 0, -1):
                        event = self._load(event_id)
                        if event and self._claimable(event, now):
                            return True
            if deadline is not None and time.monotonic() >= deadline:
                return False
            delay = self.poll_interval
            if deadline is not None:
                delay = min(delay, max(0, deadline - time.monotonic()))
            time.sleep(delay)

    @staticmethod
    def _public(event: Dict[str, Any]) -> Dict[str, Any]:
        result = dict(event)
        for field in ("status", "consumer", "lease_until", "acknowledged_at", "_sequence"):
            result.pop(field, None)
        return result

    def purge(self) -> int:
        """Delete every mailbox key in this namespace. Tests/admin only."""
        deleted = 0
        for key in self._r.scan_iter(f"{self.namespace}:*"):
            if key != f"{self.namespace}:lock":
                deleted += self._r.delete(key)
        return deleted
