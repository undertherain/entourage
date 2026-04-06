"""
SQLite-backed persistence for Entourage execution graphs.

Only the runtime touches this — external services talk to the queue only.
"""

import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


DEFAULT_DB_PATH = Path("data/entourage.db")


class GraphStore:
    """SQLite-backed storage for sessions, executions, and edges."""

    def __init__(self, db_path: Path = DEFAULT_DB_PATH):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self.conn = sqlite3.connect(str(db_path))
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._create_tables()

    def _create_tables(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS sessions (
                id TEXT PRIMARY KEY,
                trigger TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                initial_state TEXT NOT NULL DEFAULT '{}',
                created_at REAL NOT NULL,
                completed_at REAL
            );

            CREATE TABLE IF NOT EXISTS executions (
                id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL REFERENCES sessions(id),
                node_name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                input_state TEXT,
                result_state TEXT,
                created_at REAL NOT NULL,
                started_at REAL,
                completed_at REAL
            );

            CREATE TABLE IF NOT EXISTS edges (
                session_id TEXT NOT NULL REFERENCES sessions(id),
                from_exec_id TEXT NOT NULL,
                to_exec_id TEXT NOT NULL,
                condition TEXT,
                PRIMARY KEY (from_exec_id, to_exec_id)
            );

            CREATE INDEX IF NOT EXISTS idx_exec_session ON executions(session_id);
            CREATE INDEX IF NOT EXISTS idx_exec_status ON executions(status);
            CREATE INDEX IF NOT EXISTS idx_edges_to ON edges(to_exec_id);
            CREATE INDEX IF NOT EXISTS idx_edges_from ON edges(from_exec_id);
        """)
        self.conn.commit()

    # ── Sessions ──────────────────────────────────────────────

    def create_session(
        self, trigger: str, initial_state: Dict[str, Any]
    ) -> str:
        session_id = uuid.uuid4().hex
        self.conn.execute(
            "INSERT INTO sessions (id, trigger, status, initial_state, created_at) "
            "VALUES (?, ?, 'running', ?, ?)",
            (session_id, trigger, json.dumps(initial_state), time.time()),
        )
        self.conn.commit()
        return session_id

    def get_session(self, session_id: str) -> Optional[Dict]:
        row = self.conn.execute(
            "SELECT * FROM sessions WHERE id = ?", (session_id,)
        ).fetchone()
        if row:
            d = dict(row)
            d["initial_state"] = json.loads(d["initial_state"])
            return d
        return None

    def complete_session(self, session_id: str):
        self.conn.execute(
            "UPDATE sessions SET status = 'completed', completed_at = ? WHERE id = ?",
            (time.time(), session_id),
        )
        self.conn.commit()

    def fail_session(self, session_id: str):
        self.conn.execute(
            "UPDATE sessions SET status = 'failed', completed_at = ? WHERE id = ?",
            (time.time(), session_id),
        )
        self.conn.commit()

    def get_running_sessions(self) -> List[Dict]:
        rows = self.conn.execute(
            "SELECT * FROM sessions WHERE status = 'running'"
        ).fetchall()
        return [dict(r) for r in rows]

    # ── Executions ────────────────────────────────────────────

    def add_execution(
        self, session_id: str, node_name: str, exec_id: str = None
    ) -> str:
        if exec_id is None:
            exec_id = uuid.uuid4().hex
        self.conn.execute(
            "INSERT INTO executions (id, session_id, node_name, status, created_at) "
            "VALUES (?, ?, ?, 'pending', ?)",
            (exec_id, session_id, node_name, time.time()),
        )
        self.conn.commit()
        return exec_id

    def get_execution(self, exec_id: str) -> Optional[Dict]:
        row = self.conn.execute(
            "SELECT * FROM executions WHERE id = ?", (exec_id,)
        ).fetchone()
        if row:
            d = dict(row)
            for field in ("input_state", "result_state"):
                if d[field]:
                    d[field] = json.loads(d[field])
            return d
        return None

    def mark_running(self, exec_id: str, input_state: Dict[str, Any]):
        self.conn.execute(
            "UPDATE executions SET status = 'running', input_state = ?, started_at = ? "
            "WHERE id = ?",
            (json.dumps(input_state), time.time(), exec_id),
        )
        self.conn.commit()

    def mark_completed(self, exec_id: str, result_state: Dict[str, Any]):
        self.conn.execute(
            "UPDATE executions SET status = 'completed', result_state = ?, completed_at = ? "
            "WHERE id = ?",
            (json.dumps(result_state), time.time(), exec_id),
        )
        self.conn.commit()

    def mark_failed(self, exec_id: str, error: str = None):
        self.conn.execute(
            "UPDATE executions SET status = 'failed', result_state = ?, completed_at = ? "
            "WHERE id = ?",
            (json.dumps({"error": error}), time.time(), exec_id),
        )
        self.conn.commit()

    # ── Edges ─────────────────────────────────────────────────

    def add_edge(
        self,
        session_id: str,
        from_exec_id: str,
        to_exec_id: str,
        condition: str = None,
    ):
        self.conn.execute(
            "INSERT OR REPLACE INTO edges (session_id, from_exec_id, to_exec_id, condition) "
            "VALUES (?, ?, ?, ?)",
            (session_id, from_exec_id, to_exec_id, condition),
        )
        self.conn.commit()

    def remove_edge(self, from_exec_id: str, to_exec_id: str):
        self.conn.execute(
            "DELETE FROM edges WHERE from_exec_id = ? AND to_exec_id = ?",
            (from_exec_id, to_exec_id),
        )
        self.conn.commit()

    def get_parents(self, exec_id: str) -> List[Dict]:
        """Get all parent executions for a given execution."""
        rows = self.conn.execute(
            "SELECT e.*, ed.condition FROM executions e "
            "JOIN edges ed ON ed.from_exec_id = e.id "
            "WHERE ed.to_exec_id = ?",
            (exec_id,),
        ).fetchall()
        results = []
        for r in rows:
            d = dict(r)
            for field in ("input_state", "result_state"):
                if d[field]:
                    d[field] = json.loads(d[field])
            results.append(d)
        return results

    def get_children(self, exec_id: str) -> List[Tuple[str, Optional[str]]]:
        """Returns list of (child_exec_id, condition)."""
        rows = self.conn.execute(
            "SELECT to_exec_id, condition FROM edges WHERE from_exec_id = ?",
            (exec_id,),
        ).fetchall()
        return [(r["to_exec_id"], r["condition"]) for r in rows]

    def get_parent_exec_ids(self, exec_id: str) -> List[str]:
        rows = self.conn.execute(
            "SELECT from_exec_id FROM edges WHERE to_exec_id = ?",
            (exec_id,),
        ).fetchall()
        return [r["from_exec_id"] for r in rows]

    # ── Ready node detection ──────────────────────────────────

    def get_ready_executions(self, session_id: str) -> List[Dict]:
        """
        Find executions that are pending and whose parents are all completed.
        Also checks edge conditions against parent result states.
        """
        pending = self.conn.execute(
            "SELECT * FROM executions WHERE session_id = ? AND status = 'pending'",
            (session_id,),
        ).fetchall()

        ready = []
        for p in pending:
            parents = self.get_parents(p["id"])
            if not parents:
                # No parents = ready (e.g. first node after HEAD)
                ready.append(dict(p))
                continue

            all_done = all(par["status"] == "completed" for par in parents)
            if not all_done:
                continue

            # Check conditions on edges
            conditions_met = True
            for par in parents:
                condition = par.get("condition")
                if condition and par["result_state"]:
                    if not par["result_state"].get(condition):
                        conditions_met = False
                        break

            if conditions_met:
                ready.append(dict(p))

        return ready

    def collect_input_state(self, exec_id: str) -> Dict[str, Any]:
        """Collect merged input state from all completed parents."""
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
        # Multiple parents (parallel merge) — combine dicts
        merged = {}
        for s in states:
            merged.update(s)
        return merged

    # ── Graph rewiring (for dynamic plan injection) ───────────

    def get_children_exec_ids(self, exec_id: str) -> List[str]:
        rows = self.conn.execute(
            "SELECT to_exec_id FROM edges WHERE from_exec_id = ?",
            (exec_id,),
        ).fetchall()
        return [r["to_exec_id"] for r in rows]

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
        # Get current children of parent
        children = self.get_children(parent_exec_id)

        # Remove old edges from parent to children
        for child_id, _ in children:
            self.remove_edge(parent_exec_id, child_id)

        # Add edges: parent → plan starts
        for start_id in plan_start_ids:
            self.add_edge(session_id, parent_exec_id, start_id)

        # Add edges: plan end → original children (preserve conditions)
        for child_id, condition in children:
            self.add_edge(session_id, plan_end_id, child_id, condition)

    # ── Utilities ─────────────────────────────────────────────

    def get_session_graph(self, session_id: str) -> Dict:
        """Get full graph for debugging/visualization."""
        execs = self.conn.execute(
            "SELECT * FROM executions WHERE session_id = ?", (session_id,)
        ).fetchall()
        edges = self.conn.execute(
            "SELECT * FROM edges WHERE session_id = ?", (session_id,)
        ).fetchall()
        return {
            "executions": [dict(e) for e in execs],
            "edges": [dict(e) for e in edges],
        }

    def close(self):
        self.conn.close()
