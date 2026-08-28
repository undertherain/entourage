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
  {ns}:terminal             ZSET  terminal session ids by completed_at
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

    def _terminal_key(self) -> str:
        return f"{self.namespace}:terminal"

    def _ekey(self, eid: str) -> str:
        return f"{self.namespace}:exec:{eid}"

    def _execs_key(self, sid: str) -> str:
        return f"{self.namespace}:execs:{sid}"

    def _pending_key(self, sid: str) -> str:
        return f"{self.namespace}:pending:{sid}"

    def _outbox_key(self) -> str:
        return f"{self.namespace}:outbox"

    def _waiting_key(self) -> str:
        return f"{self.namespace}:waiting"

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
            "effects": json.loads(d["effects"]) if d.get("effects") else None,
            "wake": json.loads(d["wake"]) if d.get("wake") else None,
        }
        for f in _JSON_FIELDS:
            out[f] = json.loads(d[f]) if d.get(f) else None
        for f in _FLOAT_FIELDS:
            out[f] = float(d[f]) if d.get(f) else None
        return out

    # ── Sessions ──────────────────────────────────────────────

    def create_session(
        self,
        trigger: str,
        initial_state: Dict[str, Any],
        serial_key: str = None,
        session_id: str = None,
    ) -> Optional[str]:
        if session_id is None:
            session_id = uuid.uuid4().hex
        elif self._r.exists(self._skey(session_id)):
            return None
        if serial_key is not None:
            created = self._r.eval(
                """
                if redis.call('exists', KEYS[1]) == 1 then return 0 end
                redis.call('set', KEYS[1], ARGV[1])
                redis.call('hset', KEYS[2],
                    'id', ARGV[1], 'trigger', ARGV[2], 'status', 'running',
                    'initial_state', ARGV[3], 'serial_key', ARGV[4],
                    'created_at', ARGV[5])
                redis.call('sadd', KEYS[3], ARGV[1])
                return 1
                """,
                3,
                f"{self.namespace}:serial:{serial_key}",
                self._skey(session_id),
                self._running_key(),
                session_id,
                trigger,
                json.dumps(initial_state),
                serial_key,
                time.time(),
            )
            return session_id if created else None
        pipe = self._r.pipeline()
        pipe.hset(self._skey(session_id), mapping={
            "id": session_id,
            "trigger": trigger,
            "status": "running",
            "initial_state": json.dumps(initial_state),
            "serial_key": "",
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
        d["serial_key"] = d.get("serial_key") or None
        d["created_at"] = float(d["created_at"])
        d["completed_at"] = float(d["completed_at"]) if d.get("completed_at") else None
        return d

    def _finish_session(self, session_id: str, status: str):
        session = self.get_session(session_id)
        completed_at = time.time()
        pipe = self._r.pipeline()
        pipe.hset(self._skey(session_id), mapping={
            "status": status,
            "completed_at": completed_at,
        })
        pipe.srem(self._running_key(), session_id)
        pipe.zadd(self._terminal_key(), {session_id: completed_at})
        if session and session.get("serial_key"):
            pipe.delete(f"{self.namespace}:serial:{session['serial_key']}")
        pipe.execute()

    def complete_session(self, session_id: str):
        self._finish_session(session_id, "completed")

    def fail_session(self, session_id: str):
        self._finish_session(session_id, "failed")

    def get_running_sessions(self) -> List[Dict]:
        ids = self._r.smembers(self._running_key()) or set()
        return [s for s in (self.get_session(sid) for sid in sorted(ids)) if s]

    def get_terminal_sessions(self) -> List[Dict]:
        ids = self._r.zrange(self._terminal_key(), 0, -1) or []
        return [session for session in (self.get_session(sid) for sid in ids) if session]

    def delete_terminal_session(self, session_id: str) -> bool:
        session = self.get_session(session_id)
        if session is None:
            self._r.zrem(self._terminal_key(), session_id)
            return False
        if session["status"] not in {"completed", "failed"}:
            raise ValueError(f"session {session_id} is not terminal")
        execution_ids = sorted(self._r.smembers(self._execs_key(session_id)) or set())
        keys = [self._skey(session_id), self._execs_key(session_id), self._pending_key(session_id)]
        for exec_id in execution_ids:
            keys.extend([
                self._ekey(exec_id), self._parents_key(exec_id), self._children_key(exec_id)
            ])
        pipe = self._r.pipeline()
        if keys:
            pipe.delete(*keys)
        if execution_ids:
            pipe.srem(self._outbox_key(), *execution_ids)
            pipe.srem(self._waiting_key(), *execution_ids)
        pipe.srem(self._running_key(), session_id)
        pipe.zrem(self._terminal_key(), session_id)
        if session.get("serial_key"):
            pipe.delete(f"{self.namespace}:serial:{session['serial_key']}")
        pipe.execute()
        return True

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

    def mark_waiting(self, exec_id: str, wake: Dict[str, Any]):
        session_id = self._r.hget(self._ekey(exec_id), "session_id")
        pipe = self._r.pipeline()
        pipe.hset(self._ekey(exec_id), mapping={
            "status": "waiting",
            "wake": json.dumps(wake),
        })
        if session_id:
            pipe.srem(self._pending_key(session_id), exec_id)
        pipe.sadd(self._waiting_key(), exec_id)
        pipe.execute()

    def wake_execution(self, exec_id: str) -> bool:
        # Atomic waiting → pending: two wakers racing must produce one
        # transition, so the status check and flip happen in one script.
        woken = self._r.eval(
            """
            if redis.call('hget', KEYS[1], 'status') ~= 'waiting' then
                return 0
            end
            redis.call('hset', KEYS[1], 'status', 'pending')
            redis.call('srem', KEYS[2], ARGV[1])
            local sid = redis.call('hget', KEYS[1], 'session_id')
            if sid then
                redis.call('sadd', KEYS[3] .. sid, ARGV[1])
            end
            return 1
            """,
            3,
            self._ekey(exec_id),
            self._waiting_key(),
            f"{self.namespace}:pending:",
            exec_id,
        )
        return bool(woken)

    def get_waiting_executions(self) -> List[Dict]:
        ids = self._r.smembers(self._waiting_key()) or set()
        return [
            ex
            for ex in self._get_executions(sorted(ids))
            if ex["status"] == "waiting"
        ]

    def set_effects(self, exec_id: str, effects=None):
        pipe = self._r.pipeline()
        if effects is None:
            pipe.hdel(self._ekey(exec_id), "effects")
            pipe.srem(self._outbox_key(), exec_id)
        else:
            pipe.hset(self._ekey(exec_id), "effects", json.dumps(effects))
            pipe.sadd(self._outbox_key(), exec_id)
        pipe.execute()

    def get_pending_effect_executions(self) -> List[Dict]:
        ids = self._r.smembers(self._outbox_key()) or set()
        return [
            ex for ex in self._get_executions(sorted(ids)) if ex.get("effects")
        ]

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

    # ── Transition commit ─────────────────────────────────────

    def commit_transition(
        self,
        exec_id: str,
        session_id: str,
        result_state: Dict[str, Any],
        staged=None,
        children=None,
        effects=None,
        spawns=None,
    ):
        """Atomic override: every write of the transition is buffered onto
        one MULTI/EXEC pipeline, so the commit lands entirely or not at all.

        Almost no reads happen inside — ``session_id`` and the pre-read
        ``children`` come from the caller, and staged execution/session ids
        were generated client-side — so the whole write-set queues before a
        single ``execute()``. The one read is the spawn existence check,
        done before the pipeline builds: replayed children are skipped
        whole (single-worker-per-namespace, like the rest of recovery).
        """
        spawns = [
            child
            for child in (spawns or [])
            if not self._r.exists(self._skey(child.session_id))
        ]
        now = time.time()
        pipe = self._r.pipeline(transaction=True)
        for child in spawns:
            pipe.hset(self._skey(child.session_id), mapping={
                "id": child.session_id,
                "trigger": child.trigger,
                "status": "running",
                "initial_state": json.dumps(child.initial_state),
                "serial_key": "",
                "created_at": now,
            })
            pipe.sadd(self._running_key(), child.session_id)
            # HEAD is born completed with the initial state; END and the
            # plan executions start pending.
            pipe.hset(self._ekey(child.head_id), mapping={
                "id": child.head_id,
                "session_id": child.session_id,
                "node_name": "__HEAD__",
                "status": "completed",
                "attempts": 0,
                "result_state": json.dumps(child.initial_state),
                "created_at": now,
                "completed_at": now,
            })
            pipe.sadd(self._execs_key(child.session_id), child.head_id)
            pending = [
                {"exec_id": child.end_id, "node_name": "__END__", "policy": None}
            ] + child.executions
            for ex in pending:
                mapping = {
                    "id": ex["exec_id"],
                    "session_id": child.session_id,
                    "node_name": ex["node_name"],
                    "status": "pending",
                    "attempts": 0,
                    "created_at": now,
                }
                if ex["policy"]:
                    mapping["policy"] = json.dumps(ex["policy"])
                pipe.hset(self._ekey(ex["exec_id"]), mapping=mapping)
                pipe.sadd(self._execs_key(child.session_id), ex["exec_id"])
                pipe.sadd(self._pending_key(child.session_id), ex["exec_id"])
            for from_id, to_id, condition in child.edges:
                pipe.hset(self._children_key(from_id), to_id, condition or "")
                pipe.hset(self._parents_key(to_id), from_id, condition or "")
        if staged is not None:
            for ex in staged.executions:
                eid = ex["exec_id"]
                mapping = {
                    "id": eid,
                    "session_id": session_id,
                    "node_name": ex["node_name"],
                    "status": "pending",
                    "attempts": 0,
                    "created_at": now,
                }
                if ex["policy"]:
                    mapping["policy"] = json.dumps(ex["policy"])
                pipe.hset(self._ekey(eid), mapping=mapping)
                pipe.sadd(self._execs_key(session_id), eid)
                pipe.sadd(self._pending_key(session_id), eid)
            for from_id, to_id, condition in staged.edges:
                pipe.hset(self._children_key(from_id), to_id, condition or "")
                pipe.hset(self._parents_key(to_id), from_id, condition or "")
            # Rewire: node → [children] becomes node → starts … end → [children]
            for child_id, _ in children or []:
                pipe.hdel(self._children_key(exec_id), child_id)
                pipe.hdel(self._parents_key(child_id), exec_id)
            for start_id in staged.starts:
                pipe.hset(self._children_key(exec_id), start_id, "")
                pipe.hset(self._parents_key(start_id), exec_id, "")
            for child_id, condition in children or []:
                pipe.hset(self._children_key(staged.end), child_id, condition or "")
                pipe.hset(self._parents_key(child_id), staged.end, condition or "")
        completion = {
            "status": "completed",
            "result_state": json.dumps(result_state),
            "completed_at": now,
        }
        if effects is not None:
            completion["effects"] = json.dumps(effects)
            pipe.sadd(self._outbox_key(), exec_id)
        pipe.hset(self._ekey(exec_id), mapping=completion)
        pipe.srem(self._pending_key(session_id), exec_id)
        pipe.execute()

    # ── Maintenance ───────────────────────────────────────────

    def purge(self) -> int:
        """Delete every key in this store's namespace. For tests/admin."""
        deleted = 0
        for key in self._r.scan_iter(f"{self.namespace}:*"):
            deleted += self._r.delete(key)
        return deleted
