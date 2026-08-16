"""
Backend-agnostic interfaces for the Entourage runtime.

Two seams:

- ``GraphStore`` — persistence of sessions, executions, and edges. Backends
  implement the CRUD primitives; the graph algebra (ready detection, input
  collection, rewiring after dynamic plan injection) is shared code defined
  here in terms of those primitives. A backend with a smarter query engine
  (SQL join, Cypher) may override the algebra methods with a single query.

- ``ReadyQueue`` — transport for pointers to ready work. In Control-by-Return
  ordering lives in the DAG, not the queue: an execution is enqueued only
  once all its fan-in parents completed, so any FIFO-ish transport with
  at-least-once delivery satisfies the contract.

Execution dicts carry at least: ``id``, ``session_id``, ``node_name``,
``status`` (pending | running | completed | failed), ``input_state``,
``result_state``, ``attempts`` (times the execution entered running),
``policy`` (per-execution retry/timeout policy dict, or None),
``last_error`` (message of the most recent failed attempt, or None) and
``retry_at`` (epoch seconds before which a retrying execution must not
run, or None).
Parent dicts returned by ``get_parents`` additionally carry the
``condition`` of the connecting edge (or None).
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple


class GraphStore(ABC):
    """Storage for sessions, executions, and edges of the execution graph."""

    # ── Sessions ──────────────────────────────────────────────

    @abstractmethod
    def create_session(
        self, trigger: str, initial_state: Dict[str, Any], serial_key: str = None
    ) -> Optional[str]:
        """Atomically create a running session and claim ``serial_key``.

        Return ``None`` when another running session owns the key.
        """

    @abstractmethod
    def get_session(self, session_id: str) -> Optional[Dict]:
        ...

    @abstractmethod
    def complete_session(self, session_id: str):
        ...

    @abstractmethod
    def fail_session(self, session_id: str):
        ...

    @abstractmethod
    def get_running_sessions(self) -> List[Dict]:
        ...

    @abstractmethod
    def get_terminal_sessions(self) -> List[Dict]:
        """Completed and failed sessions, oldest completion first."""

    @abstractmethod
    def delete_terminal_session(self, session_id: str) -> bool:
        """Delete one terminal session and its graph; refuse active sessions."""

    # ── Executions ────────────────────────────────────────────

    @abstractmethod
    def add_execution(
        self,
        session_id: str,
        node_name: str,
        exec_id: str = None,
        policy: Dict[str, Any] = None,
    ) -> str:
        """Create a pending execution; return its id.

        ``policy`` is an optional retry/timeout policy dict (keys:
        max_attempts, timeout, retry_delay) stored with the execution so
        every worker honors it.
        """

    @abstractmethod
    def get_execution(self, exec_id: str) -> Optional[Dict]:
        ...

    @abstractmethod
    def get_session_executions(
        self, session_id: str, status: str = None
    ) -> List[Dict]:
        """All executions of a session, optionally filtered by status."""

    @abstractmethod
    def mark_running(self, exec_id: str, input_state: Dict[str, Any]):
        """Set status running and increment the attempt counter."""

    @abstractmethod
    def mark_completed(self, exec_id: str, result_state: Dict[str, Any]):
        ...

    @abstractmethod
    def mark_retrying(self, exec_id: str, error: str = None, retry_at: float = None):
        """Return a failed attempt to pending for redelivery.

        Keeps the attempt counter, records ``error`` as ``last_error`` and
        ``retry_at`` as the earliest time the next attempt may run. The
        engine enforces ``retry_at`` on the execution itself, because with
        at-least-once transports a duplicate delivery (startup recovery,
        redelivery) would otherwise jump the delay.
        """

    @abstractmethod
    def mark_failed(self, exec_id: str, error: str = None):
        """Terminal failure: no further attempts will be made."""

    # ── Edges ─────────────────────────────────────────────────

    @abstractmethod
    def add_edge(
        self,
        session_id: str,
        from_exec_id: str,
        to_exec_id: str,
        condition: str = None,
    ):
        """Add (or replace) the edge from_exec_id → to_exec_id."""

    @abstractmethod
    def remove_edge(self, from_exec_id: str, to_exec_id: str):
        ...

    @abstractmethod
    def get_parents(self, exec_id: str) -> List[Dict]:
        """Parent execution dicts, each with the connecting edge's ``condition``."""

    @abstractmethod
    def get_children(self, exec_id: str) -> List[Tuple[str, Optional[str]]]:
        """List of (child_exec_id, edge condition)."""

    @abstractmethod
    def get_session_edges(self, session_id: str) -> List[Dict]:
        """All edges of a session as dicts with from_exec_id/to_exec_id/condition."""

    def close(self):
        pass

    # ── Shared graph algebra ──────────────────────────────────
    # One implementation for all backends, expressed via the primitives above.

    def get_parent_exec_ids(self, exec_id: str) -> List[str]:
        return [p["id"] for p in self.get_parents(exec_id)]

    def get_children_exec_ids(self, exec_id: str) -> List[str]:
        return [child_id for child_id, _ in self.get_children(exec_id)]

    def get_ready_executions(self, session_id: str) -> List[Dict]:
        """
        Executions that are pending and whose parents are all completed,
        with all edge conditions satisfied against parent result states.
        """
        ready = []
        for ex in self.get_session_executions(session_id, status="pending"):
            parents = self.get_parents(ex["id"])
            if not parents:
                # No parents = ready (e.g. first node after HEAD)
                ready.append(ex)
                continue

            if not all(p["status"] == "completed" for p in parents):
                continue

            conditions_met = True
            for p in parents:
                condition = p.get("condition")
                if condition and p["result_state"]:
                    if not p["result_state"].get(condition):
                        conditions_met = False
                        break

            if conditions_met:
                ready.append(ex)
        return ready

    def collect_input_state(self, exec_id: str) -> Dict[str, Any]:
        """Merged input state from all completed parents."""
        parents = self.get_parents(exec_id)
        if not parents:
            # Root node — use session initial state
            ex = self.get_execution(exec_id)
            session = self.get_session(ex["session_id"])
            return session["initial_state"]

        states = [p["result_state"] for p in parents if p["result_state"]]
        if len(states) == 0:
            return {}
        if len(states) == 1:
            return states[0]
        # Multiple parents (parallel merge) — combine dicts.
        # NOTE: last-writer-wins; principled merge semantics are a known gap.
        merged = {}
        for s in states:
            merged.update(s)
        return merged

    def rewire_after_plan_injection(
        self,
        parent_exec_id: str,
        plan_start_ids: List[str],
        plan_end_id: str,
        session_id: str,
    ):
        """
        Rewire edges after injecting a plan between a node and its successors.

        Before: parent → [children]
        After:  parent → [plan_starts] → ... → plan_end → [children]
        """
        children = self.get_children(parent_exec_id)

        for child_id, _ in children:
            self.remove_edge(parent_exec_id, child_id)

        for start_id in plan_start_ids:
            self.add_edge(session_id, parent_exec_id, start_id)

        # Plan end → original children (preserve conditions)
        for child_id, condition in children:
            self.add_edge(session_id, plan_end_id, child_id, condition)

    def get_session_graph(self, session_id: str) -> Dict:
        """Full graph for debugging/visualization."""
        return {
            "executions": self.get_session_executions(session_id),
            "edges": self.get_session_edges(session_id),
        }

    # ── Transition commit ─────────────────────────────────────

    def commit_transition(
        self,
        exec_id: str,
        session_id: str,
        result_state: Dict[str, Any],
        staged=None,
        children: List[Tuple[str, Optional[str]]] = None,
    ):
        """Commit a node's returned transition as one unit.

        A node's return value proposes a transition: its result state, an
        optional plan to splice in after it, and the rewiring of its
        outgoing edges around that plan. Committing those as independently
        durable writes creates a silent-loss window — completed-but-plan-
        lost is the worst case, because the idempotence guard (only pending
        work runs) then correctly refuses to re-run the node, and the
        session flows on without the work the node scheduled.

        ``staged`` is a ``StagedPlan`` (or ``None`` for a plain completion);
        ``children`` is the node's pre-read outgoing edge list from
        ``get_children``, passed in so atomic overrides need no reads inside
        their transaction scope.

        This base implementation applies the primitives sequentially and is
        NOT atomic. It orders writes so that a crash mid-commit re-runs the
        node instead of silently truncating (``mark_completed`` goes last),
        but a partial commit can still leave orphaned pending executions.
        Backends that persist beyond the process lifetime must override
        this with a real transaction (see the SQLite and Redis stores); for
        the in-memory store non-atomicity is acceptable because the store
        dies with the process anyway.
        """
        if staged is not None:
            for ex in staged.executions:
                self.add_execution(
                    session_id,
                    ex["node_name"],
                    exec_id=ex["exec_id"],
                    policy=ex["policy"],
                )
            for from_id, to_id, condition in staged.edges:
                self.add_edge(session_id, from_id, to_id, condition)
            # Rewire: node → [children] becomes node → starts … end → [children]
            for child_id, _ in children or []:
                self.remove_edge(exec_id, child_id)
            for start_id in staged.starts:
                self.add_edge(session_id, exec_id, start_id)
            for child_id, condition in children or []:
                self.add_edge(session_id, staged.end, child_id, condition)
        self.mark_completed(exec_id, result_state)


class QueueMessage(ABC):
    """A received queue message. ``payload`` is the decoded dict."""

    payload: Dict[str, Any]

    @abstractmethod
    def ack(self):
        """Acknowledge successful processing (message is consumed)."""

    @abstractmethod
    def nack(self):
        """Give the message back for redelivery."""


class ReadyQueue(ABC):
    """Transport for trigger and ready-execution messages."""

    @abstractmethod
    def send(self, payload: Dict[str, Any]):
        ...

    @abstractmethod
    def receive(
        self, max_messages: int = 10, wait_seconds: float = 10
    ) -> List[QueueMessage]:
        """Return up to max_messages, waiting up to wait_seconds if empty."""
