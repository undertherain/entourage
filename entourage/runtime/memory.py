"""
In-memory backends — the trivial GraphStore/ReadyQueue implementations.

Used for local runs, debugging, and as the reference implementation the
test suite checks every other backend against. States are deep-copied at
the store boundary so backends with a serialization boundary (SQLite JSON,
Redis, ...) and this one behave identically.
"""

import copy
import queue
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from .interfaces import GraphStore, QueueMessage, ReadyQueue


class InMemoryGraphStore(GraphStore):
    def __init__(self):
        self._sessions: Dict[str, Dict] = {}
        self._executions: Dict[str, Dict] = {}
        # keyed (from, to) to mirror the SQL primary key / INSERT OR REPLACE
        self._edges: Dict[Tuple[str, str], Dict] = {}

    # ── Sessions ──────────────────────────────────────────────

    def create_session(self, trigger: str, initial_state: Dict[str, Any]) -> str:
        session_id = uuid.uuid4().hex
        self._sessions[session_id] = {
            "id": session_id,
            "trigger": trigger,
            "status": "running",
            "initial_state": copy.deepcopy(initial_state),
            "created_at": time.time(),
            "completed_at": None,
        }
        return session_id

    def get_session(self, session_id: str) -> Optional[Dict]:
        s = self._sessions.get(session_id)
        return copy.deepcopy(s) if s else None

    def complete_session(self, session_id: str):
        s = self._sessions[session_id]
        s["status"] = "completed"
        s["completed_at"] = time.time()

    def fail_session(self, session_id: str):
        s = self._sessions[session_id]
        s["status"] = "failed"
        s["completed_at"] = time.time()

    def get_running_sessions(self) -> List[Dict]:
        return [
            copy.deepcopy(s)
            for s in self._sessions.values()
            if s["status"] == "running"
        ]

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
        self._executions[exec_id] = {
            "id": exec_id,
            "session_id": session_id,
            "node_name": node_name,
            "status": "pending",
            "input_state": None,
            "result_state": None,
            "attempts": 0,
            "policy": copy.deepcopy(policy) if policy else None,
            "last_error": None,
            "retry_at": None,
            "created_at": time.time(),
            "started_at": None,
            "completed_at": None,
        }
        return exec_id

    def get_execution(self, exec_id: str) -> Optional[Dict]:
        ex = self._executions.get(exec_id)
        return copy.deepcopy(ex) if ex else None

    def get_session_executions(
        self, session_id: str, status: str = None
    ) -> List[Dict]:
        return [
            copy.deepcopy(ex)
            for ex in self._executions.values()
            if ex["session_id"] == session_id
            and (status is None or ex["status"] == status)
        ]

    def mark_running(self, exec_id: str, input_state: Dict[str, Any]):
        ex = self._executions[exec_id]
        ex["status"] = "running"
        ex["input_state"] = copy.deepcopy(input_state)
        ex["attempts"] = ex.get("attempts", 0) + 1
        ex["started_at"] = time.time()

    def mark_completed(self, exec_id: str, result_state: Dict[str, Any]):
        ex = self._executions[exec_id]
        ex["status"] = "completed"
        ex["result_state"] = copy.deepcopy(result_state)
        ex["completed_at"] = time.time()

    def mark_retrying(self, exec_id: str, error: str = None, retry_at: float = None):
        ex = self._executions[exec_id]
        ex["status"] = "pending"
        ex["last_error"] = error
        ex["retry_at"] = retry_at

    def mark_failed(self, exec_id: str, error: str = None):
        ex = self._executions[exec_id]
        ex["status"] = "failed"
        ex["result_state"] = {"error": error}
        ex["last_error"] = error
        ex["completed_at"] = time.time()

    # ── Edges ─────────────────────────────────────────────────

    def add_edge(
        self,
        session_id: str,
        from_exec_id: str,
        to_exec_id: str,
        condition: str = None,
    ):
        self._edges[(from_exec_id, to_exec_id)] = {
            "session_id": session_id,
            "from_exec_id": from_exec_id,
            "to_exec_id": to_exec_id,
            "condition": condition,
        }

    def remove_edge(self, from_exec_id: str, to_exec_id: str):
        self._edges.pop((from_exec_id, to_exec_id), None)

    def get_parents(self, exec_id: str) -> List[Dict]:
        results = []
        for edge in self._edges.values():
            if edge["to_exec_id"] == exec_id:
                parent = self.get_execution(edge["from_exec_id"])
                if parent:
                    parent["condition"] = edge["condition"]
                    results.append(parent)
        return results

    def get_children(self, exec_id: str) -> List[Tuple[str, Optional[str]]]:
        return [
            (edge["to_exec_id"], edge["condition"])
            for edge in self._edges.values()
            if edge["from_exec_id"] == exec_id
        ]

    def get_session_edges(self, session_id: str) -> List[Dict]:
        return [
            dict(edge)
            for edge in self._edges.values()
            if edge["session_id"] == session_id
        ]


class _InMemoryMessage(QueueMessage):
    def __init__(self, q: "InMemoryReadyQueue", payload: Dict[str, Any]):
        self._q = q
        self.payload = payload

    def ack(self):
        pass

    def nack(self):
        self._q.send(self.payload)


class InMemoryReadyQueue(ReadyQueue):
    def __init__(self):
        self._q: queue.Queue = queue.Queue()

    def send(self, payload: Dict[str, Any]):
        self._q.put(copy.deepcopy(payload))

    def receive(
        self, max_messages: int = 10, wait_seconds: float = 1
    ) -> List[QueueMessage]:
        messages = []
        try:
            payload = self._q.get(timeout=wait_seconds)
            messages.append(_InMemoryMessage(self, payload))
        except queue.Empty:
            return []
        while len(messages) < max_messages:
            try:
                payload = self._q.get_nowait()
                messages.append(_InMemoryMessage(self, payload))
            except queue.Empty:
                break
        return messages
