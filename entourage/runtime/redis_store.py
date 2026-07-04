"""
Redis-backed GraphStore.

Everything the graph algebra asks of a backend is a one-hop query
(parents/children of a node, pending executions of a session), so a
key-value store with sets and hashes is a sufficient "graph database".
Combined with the fair-share RedisReadyQueue, the entire runtime state —
execution graph, ready queue, fairness bookkeeping — fits in one Redis
instance shared by any number of stateless workers.

Key layout (under the configurable namespace):

  {ns}:session:{sid}        HASH  session fields (initial_state as JSON)
  {ns}:running              SET   session ids with status running
  {ns}:exec:{eid}           HASH  execution fields (states as JSON)
  {ns}:execs:{sid}          SET   execution ids of a session
  {ns}:pending:{sid}        SET   pending execution ids (ready-detection hot path)
  {ns}:parents:{eid}        HASH  parent exec id -> edge condition ("" = none)
  {ns}:children:{eid}       HASH  child exec id  -> edge condition ("" = none)

Durability note: Redis persistence is what you configure it to be — with
AOF `everysec` a hard crash can lose up to a second of state transitions.
Executions are idempotent and startup recovery re-enqueues ready work, so
lost transitions are re-derived rather than fatal.
"""

import json
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from .interfaces import GraphStore

_JSON_FIELDS = ("input_state", "result_state")
_FLOAT_FIELDS = ("created_at", "started_at", "completed_at", "retry_at")


class RedisGraphStore(GraphStore):
    def __init__(
        self,
        client=None,
        url: str = "redis://localhost:6379/0",
        namespace: str = "entourage:graph",
    ):
        if client is None:
            import redis

            client = redis.Redis.from_url(url, decode_responses=True)
        self._r = client
        self.namespace = namespace

    # ── Keys ──────────────────────────────────────────────────

    def _skey(self, sid: str) -> str:
        return f"{self.namespace}:session:{sid}"

    def _running_key(self) -> str:
        return f"{self.namespace}:running"

    def _ekey(self, eid: str) -> str:
        return f"{self.namespace}:exec:{eid}"

    def _execs_key(self, sid: str) -> str:
        return f"{self.namespace}:execs:{sid}"

    def _pending_key(self, sid: str) -> str:
        return f"{self.namespace}:pending:{sid}"

    def _parents_key(self, eid: str) -> str:
        return f"{self.namespace}:parents:{eid}"

    def _children_key(self, eid: str) -> str:
        return f"{self.namespace}:children:{eid}"

    # ── Decoding ──────────────────────────────────────────────

    @staticmethod
    def _decode_execution(d: Dict[str, str]) -> Optional[Dict]:
        if not d:
            return None
        out: Dict[str, Any] = {
            "id": d["id"],
            "session_id": d["session_id"],
            "node_name": d["node_name"],
            "status": d["status"],
            "attempts": int(d.get("attempts", 0)),
            "policy": json.loads(d["policy"]) if d.get("policy") else None,
            "last_error": d.get("last_error") or None,
        }
        for f in _JSON_FIELDS:
            out[f] = json.loads(d[f]) if d.get(f) else None
        for f in _FLOAT_FIELDS:
            out[f] = float(d[f]) if d.get(f) else None
        return out

    # ── Sessions ──────────────────────────────────────────────

    def create_session(self, trigger: str, initial_state: Dict[str, Any]) -> str:
        session_id = uuid.uuid4().hex
        pipe = self._r.pipeline()
        pipe.hset(self._skey(session_id), mapping={
            "id": session_id,
            "trigger": trigger,
            "status": "running",
            "initial_state": json.dumps(initial_state),
            "created_at": time.time(),
        })
        pipe.sadd(self._running_key(), session_id)
        pipe.execute()
        return session_id

    def get_session(self, session_id: str) -> Optional[Dict]:
        d = self._r.hgetall(self._skey(session_id))
        if not d:
            return None
        d["initial_state"] = json.loads(d["initial_state"])
        d["created_at"] = float(d["created_at"])
        d["completed_at"] = float(d["completed_at"]) if d.get("completed_at") else None
        return d

    def _finish_session(self, session_id: str, status: str):
        pipe = self._r.pipeline()
        pipe.hset(self._skey(session_id), mapping={
            "status": status,
            "completed_at": time.time(),
        })
        pipe.srem(self._running_key(), session_id)
        pipe.execute()

    def complete_session(self, session_id: str):
        self._finish_session(session_id, "completed")

    def fail_session(self, session_id: str):
        self._finish_session(session_id, "failed")

    def get_running_sessions(self) -> List[Dict]:
        ids = self._r.smembers(self._running_key()) or set()
        return [s for s in (self.get_session(sid) for sid in sorted(ids)) if s]

    # ── Executions ────────────────────────────────────────────

    def add_execution(
        self,
        session_id: str,
        node_name: str,
        exec_id: str = None,
        policy: Dict[str, Any] = None,
    ) -> str:
        if exec_id is None:
            exec_id = uuid.uuid4().hex
        mapping = {
            "id": exec_id,
            "session_id": session_id,
            "node_name": node_name,
            "status": "pending",
            "attempts": 0,
            "created_at": time.time(),
        }
        if policy:
            mapping["policy"] = json.dumps(policy)
        pipe = self._r.pipeline()
        pipe.hset(self._ekey(exec_id), mapping=mapping)
        pipe.sadd(self._execs_key(session_id), exec_id)
        pipe.sadd(self._pending_key(session_id), exec_id)
        pipe.execute()
        return exec_id

    def get_execution(self, exec_id: str) -> Optional[Dict]:
        return self._decode_execution(self._r.hgetall(self._ekey(exec_id)))

    def _get_executions(self, exec_ids) -> List[Dict]:
        exec_ids = list(exec_ids)
        if not exec_ids:
            return []
        pipe = self._r.pipeline()
        for eid in exec_ids:
            pipe.hgetall(self._ekey(eid))
        return [ex for ex in map(self._decode_execution, pipe.execute()) if ex]

    def get_session_executions(
        self, session_id: str, status: str = None
    ) -> List[Dict]:
        if status == "pending":
            # Hot path for ready detection — its own set, no scan-and-filter.
            ids = self._r.smembers(self._pending_key(session_id)) or set()
            return self._get_executions(sorted(ids))
        ids = self._r.smembers(self._execs_key(session_id)) or set()
        execs = self._get_executions(sorted(ids))
        if status is not None:
            execs = [ex for ex in execs if ex["status"] == status]
        return execs

    def _set_status(
        self, exec_id: str, mapping: Dict[str, Any], incr_attempts: bool = False
    ):
        session_id = self._r.hget(self._ekey(exec_id), "session_id")
        pipe = self._r.pipeline()
        pipe.hset(self._ekey(exec_id), mapping=mapping)
        if incr_attempts:
            pipe.hincrby(self._ekey(exec_id), "attempts", 1)
        if session_id:
            pipe.srem(self._pending_key(session_id), exec_id)
        pipe.execute()

    def mark_running(self, exec_id: str, input_state: Dict[str, Any]):
        self._set_status(exec_id, {
            "status": "running",
            "input_state": json.dumps(input_state),
            "started_at": time.time(),
        }, incr_attempts=True)

    def mark_completed(self, exec_id: str, result_state: Dict[str, Any]):
        self._set_status(exec_id, {
            "status": "completed",
            "result_state": json.dumps(result_state),
            "completed_at": time.time(),
        })

    def mark_retrying(self, exec_id: str, error: str = None, retry_at: float = None):
        session_id = self._r.hget(self._ekey(exec_id), "session_id")
        pipe = self._r.pipeline()
        pipe.hset(self._ekey(exec_id), mapping={
            "status": "pending",
            "last_error": error or "",
            "retry_at": retry_at or "",
        })
        if session_id:
            pipe.sadd(self._pending_key(session_id), exec_id)
        pipe.execute()

    def mark_failed(self, exec_id: str, error: str = None):
        self._set_status(exec_id, {
            "status": "failed",
            "result_state": json.dumps({"error": error}),
            "last_error": error or "",
            "completed_at": time.time(),
        })

    # ── Edges ─────────────────────────────────────────────────
    # Conditions are stored on both directions; "" encodes None.

    def add_edge(
        self,
        session_id: str,
        from_exec_id: str,
        to_exec_id: str,
        condition: str = None,
    ):
        pipe = self._r.pipeline()
        pipe.hset(self._children_key(from_exec_id), to_exec_id, condition or "")
        pipe.hset(self._parents_key(to_exec_id), from_exec_id, condition or "")
        pipe.execute()

    def remove_edge(self, from_exec_id: str, to_exec_id: str):
        pipe = self._r.pipeline()
        pipe.hdel(self._children_key(from_exec_id), to_exec_id)
        pipe.hdel(self._parents_key(to_exec_id), from_exec_id)
        pipe.execute()

    def get_parents(self, exec_id: str) -> List[Dict]:
        edges = self._r.hgetall(self._parents_key(exec_id)) or {}
        parents = self._get_executions(sorted(edges))
        for p in parents:
            p["condition"] = edges[p["id"]] or None
        return parents

    def get_children(self, exec_id: str) -> List[Tuple[str, Optional[str]]]:
        edges = self._r.hgetall(self._children_key(exec_id)) or {}
        return [(child_id, cond or None) for child_id, cond in sorted(edges.items())]

    def get_session_edges(self, session_id: str) -> List[Dict]:
        exec_ids = sorted(self._r.smembers(self._execs_key(session_id)) or set())
        pipe = self._r.pipeline()
        for eid in exec_ids:
            pipe.hgetall(self._children_key(eid))
        edges = []
        for eid, children in zip(exec_ids, pipe.execute()):
            for child_id, cond in (children or {}).items():
                edges.append({
                    "session_id": session_id,
                    "from_exec_id": eid,
                    "to_exec_id": child_id,
                    "condition": cond or None,
                })
        return edges

    # ── Maintenance ───────────────────────────────────────────

    def purge(self) -> int:
        """Delete every key in this store's namespace. For tests/admin."""
        deleted = 0
        for key in self._r.scan_iter(f"{self.namespace}:*"):
            deleted += self._r.delete(key)
        return deleted
