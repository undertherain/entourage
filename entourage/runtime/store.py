"""
SQLite-backed GraphStore.

Implements only the storage primitives; the graph algebra (ready detection,
input collection, rewiring) is inherited from the GraphStore interface.
Only the runtime touches this — external services talk to the queue only.
"""

import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .interfaces import GraphStore


DEFAULT_DB_PATH = Path("data/entourage.db")


class SQLiteGraphStore(GraphStore):
    """SQLite-backed storage for sessions, executions, and edges."""

    def __init__(self, db_path: Path = DEFAULT_DB_PATH):
        db_path = Path(db_path)
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
                attempts INTEGER NOT NULL DEFAULT 0,
                policy TEXT,
                last_error TEXT,
                retry_at REAL,
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
        # Migrate pre-retry-policy databases in place.
        existing = {
            r["name"]
            for r in self.conn.execute("PRAGMA table_info(executions)")
        }
        for column, decl in (
            ("attempts", "INTEGER NOT NULL DEFAULT 0"),
            ("policy", "TEXT"),
            ("last_error", "TEXT"),
            ("retry_at", "REAL"),
        ):
            if column not in existing:
                self.conn.execute(
                    f"ALTER TABLE executions ADD COLUMN {column} {decl}"
                )
        self.conn.commit()

    @staticmethod
    def _decode_execution(row) -> Dict:
        d = dict(row)
        for field in ("input_state", "result_state", "policy"):
            if d.get(field):
                d[field] = json.loads(d[field])
        return d

    # ── Sessions ──────────────────────────────────────────────

    def create_session(self, trigger: str, initial_state: Dict[str, Any]) -> str:
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
        self,
        session_id: str,
        node_name: str,
        exec_id: str = None,
        policy: Dict[str, Any] = None,
    ) -> str:
        if exec_id is None:
            exec_id = uuid.uuid4().hex
        self.conn.execute(
            "INSERT INTO executions (id, session_id, node_name, status, policy, created_at) "
            "VALUES (?, ?, ?, 'pending', ?, ?)",
            (exec_id, session_id, node_name,
             json.dumps(policy) if policy else None, time.time()),
        )
        self.conn.commit()
        return exec_id

    def get_execution(self, exec_id: str) -> Optional[Dict]:
        row = self.conn.execute(
            "SELECT * FROM executions WHERE id = ?", (exec_id,)
        ).fetchone()
        return self._decode_execution(row) if row else None

    def get_session_executions(
        self, session_id: str, status: str = None
    ) -> List[Dict]:
        if status is None:
            rows = self.conn.execute(
                "SELECT * FROM executions WHERE session_id = ?", (session_id,)
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM executions WHERE session_id = ? AND status = ?",
                (session_id, status),
            ).fetchall()
        return [self._decode_execution(r) for r in rows]

    def mark_running(self, exec_id: str, input_state: Dict[str, Any]):
        self.conn.execute(
            "UPDATE executions SET status = 'running', input_state = ?, "
            "attempts = attempts + 1, started_at = ? WHERE id = ?",
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

    def mark_retrying(self, exec_id: str, error: str = None, retry_at: float = None):
        self.conn.execute(
            "UPDATE executions SET status = 'pending', last_error = ?, retry_at = ? "
            "WHERE id = ?",
            (error, retry_at, exec_id),
        )
        self.conn.commit()

    def mark_failed(self, exec_id: str, error: str = None):
        self.conn.execute(
            "UPDATE executions SET status = 'failed', result_state = ?, "
            "last_error = ?, completed_at = ? WHERE id = ?",
            (json.dumps({"error": error}), error, time.time(), exec_id),
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
        rows = self.conn.execute(
            "SELECT e.*, ed.condition FROM executions e "
            "JOIN edges ed ON ed.from_exec_id = e.id "
            "WHERE ed.to_exec_id = ?",
            (exec_id,),
        ).fetchall()
        return [self._decode_execution(r) for r in rows]

    def get_children(self, exec_id: str) -> List[Tuple[str, Optional[str]]]:
        rows = self.conn.execute(
            "SELECT to_exec_id, condition FROM edges WHERE from_exec_id = ?",
            (exec_id,),
        ).fetchall()
        return [(r["to_exec_id"], r["condition"]) for r in rows]

    def get_session_edges(self, session_id: str) -> List[Dict]:
        rows = self.conn.execute(
            "SELECT * FROM edges WHERE session_id = ?", (session_id,)
        ).fetchall()
        return [dict(r) for r in rows]

    def close(self):
        self.conn.close()
