"""
Redis-backed ReadyQueue with fair-share claiming.

The problem a plain FIFO queue can't solve: one chatty session enqueuing a
wide fan-out means session A's 100 ready executions drain before session B's
first one starts. Here every payload goes to a per-session list and claiming
rotates through the active sessions — a session gets at most one claim per
rotation pass, so new sessions are served within a few claims regardless of
how deep another session's backlog is.

Ported from a production system where the fairness key was the tenant; in
Entourage the natural key is the session (payloads without a session_id —
e.g. triggers — share one system key).

Key layout (all under the configurable namespace, so several runtimes can
share one Redis):

  {ns}:active       SET of fairness keys with pending payloads
  {ns}:list:{key}   LIST of JSON envelopes (LPUSH to enqueue, RPOP to claim)
  {ns}:in_flight    HASH envelope_id -> envelope JSON, claimed but not acked

Delivery is at-least-once: a claimed envelope sits in the in-flight hash
until ack; nack re-enqueues it at the front; reclaim_stale() re-enqueues
envelopes held longer than `reclaim_after` seconds (a worker died mid-task).
The runtime's idempotence guard makes duplicate delivery harmless.

Priority tiers (interactive > bulk > background) are planned at the worker
level — the layout extends by prefixing the namespace per tier.
"""

import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional

from .interfaces import QueueMessage, ReadyQueue

logger = logging.getLogger(__name__)

# Payloads older than this are almost certainly bugs; surface via key expiry
# rather than hoard them indefinitely. Refreshed on enqueue AND on claim, so
# a backlog that takes longer than the TTL to drain doesn't expire mid-drain.
QUEUE_TTL_SECONDS = 86400

# Fairness key for payloads that don't belong to a session yet (triggers).
SYSTEM_KEY = "__system__"


class _RedisMessage(QueueMessage):
    def __init__(self, queue: "RedisReadyQueue", envelope: Dict[str, Any]):
        self._queue = queue
        self._envelope = envelope
        self.payload = envelope["payload"]

    def ack(self):
        self._queue._ack(self._envelope)

    def nack(self):
        self._queue._nack(self._envelope)


class RedisReadyQueue(ReadyQueue):
    def __init__(
        self,
        client=None,
        url: str = "redis://localhost:6379/0",
        namespace: str = "entourage:queue",
        ttl_seconds: int = QUEUE_TTL_SECONDS,
        reclaim_after: float = 300.0,
        poll_interval: float = 0.1,
    ):
        if client is None:
            import redis

            client = redis.Redis.from_url(url, decode_responses=True)
        self._r = client
        self.namespace = namespace
        self.ttl_seconds = ttl_seconds
        self.reclaim_after = reclaim_after
        self.poll_interval = poll_interval
        self._last_reclaim = 0.0

    # ── Keys ──────────────────────────────────────────────────

    def _active_key(self) -> str:
        return f"{self.namespace}:active"

    def _list_key(self, key: str) -> str:
        return f"{self.namespace}:list:{key}"

    def _in_flight_key(self) -> str:
        return f"{self.namespace}:in_flight"

    @staticmethod
    def _fairness_key(payload: Dict[str, Any]) -> str:
        return payload.get("session_id") or SYSTEM_KEY

    # ── ReadyQueue interface ──────────────────────────────────

    def send(self, payload: Dict[str, Any]):
        key = self._fairness_key(payload)
        envelope = {
            "id": uuid.uuid4().hex,
            "key": key,
            "enqueued_at": time.time(),
            "payload": payload,
        }
        self._push(envelope, front=False)

    def receive(
        self, max_messages: int = 10, wait_seconds: float = 10
    ) -> List[QueueMessage]:
        deadline = time.monotonic() + wait_seconds
        self._maybe_reclaim()
        messages = self._claim_batch(max_messages)
        while not messages:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(self.poll_interval, remaining))
            messages = self._claim_batch(max_messages)
        return messages

    # ── Enqueue / claim mechanics ─────────────────────────────

    def _push(self, envelope: Dict[str, Any], front: bool):
        raw = json.dumps(envelope)
        list_key = self._list_key(envelope["key"])
        # Push the payload BEFORE marking the key active: if the key was
        # already active SADD is a no-op; if it wasn't, a claimer seeing it
        # in the set is guaranteed to find at least one message waiting.
        pipe = self._r.pipeline()
        if front:
            pipe.rpush(list_key, raw)  # RPOP end — redelivered first
        else:
            pipe.lpush(list_key, raw)
        pipe.sadd(self._active_key(), envelope["key"])
        pipe.expire(list_key, self.ttl_seconds)
        pipe.expire(self._active_key(), self.ttl_seconds)
        pipe.execute()

    def _claim_batch(self, max_messages: int) -> List[QueueMessage]:
        """Rotate over active fairness keys, claiming at most one payload
        per key per pass, until the batch is full or the queue is dry."""
        messages: List[QueueMessage] = []
        while len(messages) < max_messages:
            active = self._r.smembers(self._active_key()) or set()
            if not active:
                break
            claimed_this_pass = False
            for key in active:
                if len(messages) >= max_messages:
                    break
                envelope = self._claim_one(key)
                if envelope is not None:
                    messages.append(_RedisMessage(self, envelope))
                    claimed_this_pass = True
            if not claimed_this_pass:
                break
        return messages

    def _claim_one(self, key: str) -> Optional[Dict[str, Any]]:
        list_key = self._list_key(key)
        raw = self._r.rpop(list_key)
        if raw is None:
            # Drained key — prune it from the active set, then re-check: a
            # concurrent send may have pushed between our RPOP and SREM, and
            # its SADD may have landed before our SREM. Re-adding on a
            # non-empty list closes the stranded-message window.
            self._r.srem(self._active_key(), key)
            if self._r.llen(list_key) > 0:
                self._r.sadd(self._active_key(), key)
            return None
        try:
            envelope = json.loads(raw)
        except json.JSONDecodeError:
            logger.error("fair queue: malformed payload for key %s, discarding", key)
            return None
        # Key stays in the active set even if the list is now empty — the
        # next pass RPOPs nil and prunes it safely. Refresh TTLs while a
        # backlog is draining so it can't expire mid-drain.
        if self._r.llen(list_key) > 0:
            pipe = self._r.pipeline()
            pipe.expire(list_key, self.ttl_seconds)
            pipe.expire(self._active_key(), self.ttl_seconds)
            pipe.execute()
        envelope["claimed_at"] = time.time()
        self._r.hset(self._in_flight_key(), envelope["id"], json.dumps(envelope))
        return envelope

    # ── Ack / nack / reclaim ──────────────────────────────────

    def _ack(self, envelope: Dict[str, Any]):
        self._r.hdel(self._in_flight_key(), envelope["id"])

    def _nack(self, envelope: Dict[str, Any]):
        self._r.hdel(self._in_flight_key(), envelope["id"])
        self._push(envelope, front=True)

    def reclaim_stale(self, older_than: float = None) -> int:
        """Re-enqueue in-flight envelopes claimed longer than `older_than`
        seconds ago (their worker presumably died). Returns count reclaimed."""
        threshold = time.time() - (
            older_than if older_than is not None else self.reclaim_after
        )
        reclaimed = 0
        for env_id, raw in self._r.hscan_iter(self._in_flight_key()):
            try:
                envelope = json.loads(raw)
            except json.JSONDecodeError:
                self._r.hdel(self._in_flight_key(), env_id)
                continue
            if envelope.get("claimed_at", 0) <= threshold:
                # Delete first so two reclaimers can't both re-enqueue it.
                if self._r.hdel(self._in_flight_key(), env_id):
                    self._push(envelope, front=True)
                    reclaimed += 1
        if reclaimed:
            logger.warning("fair queue: reclaimed %d stale in-flight payloads", reclaimed)
        return reclaimed

    def _maybe_reclaim(self):
        now = time.monotonic()
        if now - self._last_reclaim >= min(self.reclaim_after, 60.0):
            self._last_reclaim = now
            self.reclaim_stale()

    # ── Introspection & maintenance ───────────────────────────

    def active_keys(self) -> List[str]:
        return sorted(self._r.smembers(self._active_key()) or set())

    def pending_count(self, key: str) -> int:
        return int(self._r.llen(self._list_key(key)) or 0)

    def in_flight_count(self) -> int:
        return int(self._r.hlen(self._in_flight_key()) or 0)

    def purge(self) -> int:
        """Delete every key in this queue's namespace. For tests/admin."""
        deleted = 0
        for key in self._r.scan_iter(f"{self.namespace}:*"):
            deleted += self._r.delete(key)
        return deleted
