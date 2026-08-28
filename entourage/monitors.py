"""Monitors — declared expectations over the event plane.

"We expect a result for that dispatch" and "we expect periodic activity
from that subagent" are one primitive with two parameterizations:

- **deadline** (one-shot): expect a matching observation by an epoch
  deadline. A matching observation satisfies and removes the monitor; a
  lapse fires once and removes it.
- **heartbeat** (sliding): expect a matching observation every
  ``interval`` seconds. Each observation refreshes the window and clears a
  previous lapse; going quiet fires one lapse per quiet spell (``cycles``
  distinguishes their idempotency keys).

Monitors are *armed inside the Transition commit* that performs the
dispatch (a third effect beside acknowledge/publish), so the expectation
cannot be lost in the window where nobody waits. Observation happens where
events already flow — ingress acceptance and outbox publication — and
matching is by ``correlation_id`` or ``source`` only, never payload.

A lapse is not a callback: the engine's tick appends a ``kind: system``
event (``source: monitor``) to the monitor's ``notify`` conversation, and
the supervisor drains it like any other mail. Evaluation is lazy — nothing
is cancelled on arrival; the tick checks state at fire time (the same
discipline as wait deadlines and completion routes).

The remote side's own promised deadline may inform ``deadline``/``interval``
but never enforces them: a dead child can't report itself dead.
"""

import copy
import json
import time
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class Monitor:
    """A declared expectation; ``monitor_id`` is its idempotency key.

    Exactly one of ``deadline`` (epoch seconds, one-shot) and ``interval``
    (seconds, sliding heartbeat) must be set. At least one of
    ``correlation_id`` and ``source`` must be set; an observation matches
    when either equals the monitor's.
    """

    notify: str
    monitor_id: Optional[str] = None
    correlation_id: Optional[str] = None
    source: Optional[str] = None
    deadline: Optional[float] = None
    interval: Optional[float] = None

    def __post_init__(self):
        if not self.notify:
            raise ValueError("Monitor needs a notify conversation")
        if (self.deadline is None) == (self.interval is None):
            raise ValueError("Monitor needs exactly one of deadline/interval")
        if self.interval is not None and self.interval <= 0:
            raise ValueError("interval must be > 0")
        if self.correlation_id is None and self.source is None:
            raise ValueError("Monitor needs correlation_id and/or source")


def _matches(record: Dict[str, Any], correlation_id, source) -> bool:
    return bool(
        (record.get("correlation_id") and record["correlation_id"] == correlation_id)
        or (record.get("source") and record["source"] == source)
    )


class MonitorStore(ABC):
    """Durable armed expectations; small, correlation/source-keyed."""

    @abstractmethod
    def arm(self, monitor: Dict[str, Any], now: Optional[float] = None) -> None:
        """Arm a monitor record; arming an existing ``monitor_id`` is a
        no-op. Replay of a committed transition's effects must neither
        double-arm nor refresh a live heartbeat's window (that would mask a
        real lapse); a deliberate re-arm is disarm + arm."""

    @abstractmethod
    def disarm(self, monitor_id: str) -> bool:
        """Remove a monitor; True when it existed."""

    @abstractmethod
    def observe(
        self,
        correlation_id: Optional[str] = None,
        source: Optional[str] = None,
        now: Optional[float] = None,
    ) -> int:
        """Feed an observation: satisfy matching deadline monitors (they
        are removed), refresh matching heartbeats (window restarts, a
        previous lapse clears). Returns how many monitors matched."""

    @abstractmethod
    def due(self, now: Optional[float] = None) -> List[Dict[str, Any]]:
        """Monitors whose expectation has lapsed and was not yet fired."""

    @abstractmethod
    def mark_lapsed(self, monitor_id: str, now: Optional[float] = None) -> None:
        """Record that the lapse fired: deadline monitors are removed,
        heartbeats stay armed with ``lapsed`` set and ``cycles`` bumped."""

    @abstractmethod
    def list(self) -> List[Dict[str, Any]]:
        ...


def _armed_record(monitor: Dict[str, Any], now: float) -> Dict[str, Any]:
    record = dict(monitor)
    record.setdefault("last_seen", now)
    record.setdefault("lapsed", False)
    record.setdefault("cycles", 0)
    return record


def _is_due(record: Dict[str, Any], now: float) -> bool:
    if record.get("lapsed"):
        return False
    if record.get("deadline") is not None:
        return now >= record["deadline"]
    return now - record["last_seen"] >= record["interval"]


class InMemoryMonitorStore(MonitorStore):
    def __init__(self):
        self._monitors: Dict[str, Dict[str, Any]] = {}

    def arm(self, monitor: Dict[str, Any], now: Optional[float] = None) -> None:
        now = time.time() if now is None else now
        existing = self._monitors.get(monitor["monitor_id"])
        if existing is not None:
            return  # replay of committed effects: already armed
        self._monitors[monitor["monitor_id"]] = _armed_record(
            copy.deepcopy(monitor), now
        )

    def disarm(self, monitor_id: str) -> bool:
        return self._monitors.pop(monitor_id, None) is not None

    def observe(self, correlation_id=None, source=None, now=None) -> int:
        now = time.time() if now is None else now
        matched = 0
        for monitor_id, record in list(self._monitors.items()):
            if not _matches(record, correlation_id, source):
                continue
            matched += 1
            if record.get("deadline") is not None:
                del self._monitors[monitor_id]  # satisfied
            else:
                record["last_seen"] = now
                record["lapsed"] = False
        return matched

    def due(self, now=None) -> List[Dict[str, Any]]:
        now = time.time() if now is None else now
        return [
            copy.deepcopy(record)
            for record in self._monitors.values()
            if _is_due(record, now)
        ]

    def mark_lapsed(self, monitor_id: str, now=None) -> None:
        record = self._monitors.get(monitor_id)
        if record is None:
            return
        if record.get("deadline") is not None:
            del self._monitors[monitor_id]  # one-shot: fired, gone
        else:
            record["lapsed"] = True
            record["cycles"] = record.get("cycles", 0) + 1

    def list(self) -> List[Dict[str, Any]]:
        return [copy.deepcopy(record) for record in self._monitors.values()]


class RedisMonitorStore(MonitorStore):
    """Monitors as hashes under one namespace; the id set is the scan index.

    Monitor counts are small (one per outstanding dispatch/child), so
    observe/due iterate the set; there is no per-correlation index to keep
    consistent."""

    def __init__(
        self,
        client=None,
        url: str = "redis://localhost:6379/0",
        namespace: str = "entourage:monitors",
    ):
        if client is None:
            import redis

            client = redis.Redis.from_url(url, decode_responses=True)
        self._r = client
        self.namespace = namespace

    def _ids_key(self) -> str:
        return f"{self.namespace}:ids"

    def _mkey(self, monitor_id: str) -> str:
        return f"{self.namespace}:m:{monitor_id}"

    def _save(self, record: Dict[str, Any]):
        pipe = self._r.pipeline()
        pipe.set(self._mkey(record["monitor_id"]), json.dumps(record))
        pipe.sadd(self._ids_key(), record["monitor_id"])
        pipe.execute()

    def _load(self, monitor_id: str) -> Optional[Dict[str, Any]]:
        value = self._r.get(self._mkey(monitor_id))
        return json.loads(value) if value else None

    def _delete(self, monitor_id: str) -> bool:
        pipe = self._r.pipeline()
        pipe.delete(self._mkey(monitor_id))
        pipe.srem(self._ids_key(), monitor_id)
        deleted, _ = pipe.execute()
        return bool(deleted)

    def arm(self, monitor: Dict[str, Any], now: Optional[float] = None) -> None:
        now = time.time() if now is None else now
        if self._load(monitor["monitor_id"]) is not None:
            return
        self._save(_armed_record(dict(monitor), now))

    def disarm(self, monitor_id: str) -> bool:
        return self._delete(monitor_id)

    def observe(self, correlation_id=None, source=None, now=None) -> int:
        now = time.time() if now is None else now
        matched = 0
        for monitor_id in sorted(self._r.smembers(self._ids_key()) or set()):
            record = self._load(monitor_id)
            if record is None or not _matches(record, correlation_id, source):
                continue
            matched += 1
            if record.get("deadline") is not None:
                self._delete(monitor_id)
            else:
                record["last_seen"] = now
                record["lapsed"] = False
                self._save(record)
        return matched

    def due(self, now=None) -> List[Dict[str, Any]]:
        now = time.time() if now is None else now
        due = []
        for monitor_id in sorted(self._r.smembers(self._ids_key()) or set()):
            record = self._load(monitor_id)
            if record is not None and _is_due(record, now):
                due.append(record)
        return due

    def mark_lapsed(self, monitor_id: str, now=None) -> None:
        record = self._load(monitor_id)
        if record is None:
            return
        if record.get("deadline") is not None:
            self._delete(monitor_id)
        else:
            record["lapsed"] = True
            record["cycles"] = record.get("cycles", 0) + 1
            self._save(record)

    def list(self) -> List[Dict[str, Any]]:
        return [
            record
            for record in (
                self._load(mid)
                for mid in sorted(self._r.smembers(self._ids_key()) or set())
            )
            if record is not None
        ]

    def purge(self) -> int:
        """Delete every key in this store's namespace. For tests/admin."""
        deleted = 0
        for key in self._r.scan_iter(f"{self.namespace}:*"):
            deleted += self._r.delete(key)
        return deleted


def monitor_to_dict(monitor: Monitor) -> Dict[str, Any]:
    return {k: v for k, v in asdict(monitor).items() if v is not None}
