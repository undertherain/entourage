"""Experimental graph-independent sessions, with a local SQLite wake binding.

The database is the ready queue: notifications are unnecessary for correctness.
An activation is explicit restored data, never a persisted Python stack. Local
publication shares the checkpoint transaction; remote delivery needs an outbox
adapter before it can use this contract. No graph or model imports are required.
"""

import json
import math
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Optional

# Re-export shared types so existing entourage.sessions imports keep working.
from .session_backend import (
    Activation, Publication, SessionAlreadyExists, SessionBackend,
    SessionSnapshot, Spawn, StaleActivation,
)


class LocalSessions(SessionBackend):
    """Single-host durable wake scheduling for bounded activations.

    Separate processes may open the same file. Transactions serialize claims,
    appends and checkpoints; expired leases are reclaimable. Callers poll
    ``claim`` and execute outside the transaction. Tables are prefixed so the
    binding may live in an existing SQLite database. This is an initial seam,
    not yet an adapter for the graph runner or a worker-pool implementation.
    """

    def __init__(self, path: Path, clock: Callable[[], float] = time.time):
        self.path = str(path)
        if self.path == ":memory:":
            raise ValueError("LocalSessions requires a file-backed database")
        self.clock = clock
        with self._transaction() as db:
            db.execute("""CREATE TABLE IF NOT EXISTS wake_sessions (
                id TEXT PRIMARY KEY, executable TEXT NOT NULL, state TEXT NOT NULL,
                status TEXT NOT NULL, deadline REAL, token TEXT, lease_until REAL,
                revision INTEGER NOT NULL DEFAULT 0
            )""")
            db.execute("""CREATE TABLE IF NOT EXISTS wake_inputs (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id TEXT NOT NULL REFERENCES wake_sessions(id),
                event_id TEXT NOT NULL, event TEXT NOT NULL,
                incorporated INTEGER NOT NULL DEFAULT 0,
                UNIQUE(session_id, event_id)
            )""")
            db.execute("""CREATE INDEX IF NOT EXISTS wake_pending
                ON wake_inputs(session_id, incorporated, seq)""")
            columns = {row[1] for row in db.execute("PRAGMA table_info(wake_sessions)")}
            if "last_claim" not in columns:
                db.execute("ALTER TABLE wake_sessions ADD COLUMN last_claim INTEGER NOT NULL DEFAULT 0")
            if "completed_at" not in columns:
                # Sessions completed before this column existed are never purged.
                db.execute("ALTER TABLE wake_sessions ADD COLUMN completed_at REAL")
            db.execute("""CREATE TABLE IF NOT EXISTS wake_definitions (
                executable TEXT PRIMARY KEY, contract TEXT NOT NULL
            )""")

    def bind_definition(self, executable: str, contract: dict) -> None:
        """Reject changes to a registered version, in the same session database."""
        encoded = json.dumps(contract, sort_keys=True, allow_nan=False)
        with self._transaction() as db:
            row = db.execute("SELECT contract FROM wake_definitions WHERE executable = ?",
                             (executable,)).fetchone()
            if row is not None and row[0] != encoded:
                raise ValueError(f"definition {executable!r} changed; register a new version")
            db.execute("INSERT OR IGNORE INTO wake_definitions VALUES (?, ?)",
                       (executable, encoded))

    def inspect(self, session_id: str) -> SessionSnapshot:
        """Read committed state/status without consuming mail or acquiring a lease."""
        with self._transaction() as db:
            row = db.execute("SELECT executable, state, status, deadline, revision "
                             "FROM wake_sessions WHERE id = ?", (session_id,)).fetchone()
            if row is None:
                raise KeyError(session_id)
            return SessionSnapshot(executable=row["executable"], state=json.loads(row["state"]),
                                   status=row["status"], deadline=row["deadline"],
                                   revision=row["revision"])

    @contextmanager
    def _transaction(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA foreign_keys = ON")
        try:
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def _create(db, session_id: str, executable: str, state: dict) -> None:
        if not isinstance(session_id, str) or not isinstance(executable, str):
            raise ValueError("session_id and executable must be strings")
        if not session_id or not executable:
            raise ValueError("session_id and executable must be nonempty")
        if not isinstance(state, dict):
            raise ValueError("state must be a JSON object")
        if db.execute("SELECT 1 FROM wake_sessions WHERE id = ?", (session_id,)).fetchone():
            raise SessionAlreadyExists(session_id)
        db.execute("""INSERT INTO wake_sessions(id, executable, state, status)
            VALUES (?, ?, ?, 'ready')""", (session_id, executable, json.dumps(state)))

    def create(self, session_id: str, executable: str, state: dict) -> None:
        """Create a runnable session; raise SessionAlreadyExists rather than reset."""
        with self._transaction() as db:
            self._create(db, session_id, executable, state)

    @staticmethod
    def _append(db, session_id: str, event: dict, *, timer: bool = False) -> bool:
        event_id = event.get("event_id")
        if not isinstance(event_id, str) or not event_id:
            raise ValueError("event requires a nonempty string event_id")
        if event_id.startswith("timer:") and not timer:
            raise ValueError("timer: event IDs are reserved for deadline delivery")
        session = db.execute(
            "SELECT status FROM wake_sessions WHERE id = ?", (session_id,)
        ).fetchone()
        if session is None:
            raise KeyError(session_id)
        # Retained input IDs also deduplicate retries after completion.
        if db.execute("SELECT 1 FROM wake_inputs WHERE session_id = ? AND event_id = ?",
                      (session_id, event_id)).fetchone():
            return False
        if session["status"] == "complete":
            raise ValueError(f"session {session_id!r} is complete")
        db.execute("INSERT INTO wake_inputs(session_id, event_id, event) VALUES (?, ?, ?)",
                   (session_id, event_id, json.dumps(event)))
        return True

    def append(self, session_id: str, event: dict) -> bool:
        """Persist mail idempotently, even when its session is currently active."""
        with self._transaction() as db:
            return self._append(db, session_id, event)

    def claim(self, *, lease_seconds: float = 30,
              executable: Optional[str] = None,
              max_events: Optional[int] = None) -> Optional[Activation]:
        """Lease ready work, including expired activations and persisted deadlines.

        A parked session with pending mail is runnable by definition. Checking
        this predicate after wait registration closes the mail-during-park race,
        and scanning it after restart reconstructs readiness without wake hints.
        Least-recently claimed sessions go first. A bounded batch sets has_more
        if additional mail was already queued; later arrivals also stay pending.
        """
        if not math.isfinite(lease_seconds) or lease_seconds <= 0:
            raise ValueError("lease_seconds must be finite and positive")
        if max_events is not None and (type(max_events) is not int or max_events <= 0):
            raise ValueError("max_events must be a positive integer")
        with self._transaction() as db:
            now = self.clock()
            row = db.execute("""SELECT * FROM wake_sessions s
                WHERE status != 'complete' AND (lease_until IS NULL OR lease_until <= ?)
                AND (? IS NULL OR executable = ?)
                AND (status IN ('ready', 'active') OR deadline <= ? OR EXISTS (
                    SELECT 1 FROM wake_inputs i
                    WHERE i.session_id = s.id AND i.incorporated = 0))
                ORDER BY s.last_claim, s.rowid LIMIT 1""", (now, executable, executable, now)).fetchone()
            if row is None:
                return None
            if row["deadline"] is not None and row["deadline"] <= now:
                self._append(db, row["id"], {
                    "event_id": f"timer:{row['id']}:{row['revision']}",
                    "kind": "system", "source": "timer",
                    "payload": {"deadline": row["deadline"]},
                }, timer=True)
            token = uuid.uuid4().hex
            db.execute("""UPDATE wake_sessions SET status = 'active', token = ?,
                lease_until = ?, last_claim = (SELECT COALESCE(MAX(last_claim), 0) + 1
                FROM wake_sessions) WHERE id = ?""", (token, now + lease_seconds, row["id"]))
            events = [json.loads(item[0]) for item in db.execute(
                """SELECT event FROM wake_inputs WHERE session_id = ?
                AND incorporated = 0 ORDER BY seq LIMIT ?""",
                (row["id"], max_events + 1 if max_events is not None else -1))]
            has_more = max_events is not None and len(events) > max_events
            if has_more:
                events = events[:max_events]
            return Activation(row["id"], row["executable"], token,
                              json.loads(row["state"]), events, has_more)

    def commit(self, activation: Activation, state: dict, *, incorporated: list[str],
               publish: tuple[Publication, ...] = (), deadline: Optional[float] = None,
               complete: bool = False, spawn: tuple[Spawn, ...] = ()) -> None:
        """Atomically checkpoint inputs, children, local outgoing mail and the next wait.

        Only inputs delivered to this activation can be incorporated. Unconsumed
        mail (including mail arriving during execution) remains ready. Children
        are created before publications so the parent can address them at once.
        Parking or completing has no child-cancellation side effects. A successful
        commit releases the lease; process exit by itself does not commit anything.
        """
        if deadline is not None and (not math.isfinite(deadline) or complete):
            raise ValueError("deadline must be finite and belong to a waiting session")
        delivered = {event["event_id"] for event in activation.events}
        if not set(incorporated) <= delivered:
            raise ValueError("cannot incorporate an input absent from this activation")
        children = [child.session_id for child in spawn]
        if len(set(children)) != len(children) or activation.session_id in children:
            raise ValueError("spawned session IDs must be distinct and differ from the parent")
        with self._transaction() as db:
            now = self.clock()
            row = db.execute("SELECT * FROM wake_sessions WHERE id = ?",
                             (activation.session_id,)).fetchone()
            if (row is None or row["token"] != activation.token
                    or row["lease_until"] is None or row["lease_until"] <= now):
                raise StaleActivation(activation.session_id)
            for child in spawn:
                if not db.execute("SELECT 1 FROM wake_definitions WHERE executable = ?",
                                  (child.executable,)).fetchone():
                    raise ValueError(f"cannot spawn unbound definition {child.executable!r}")
                self._create(db, child.session_id, child.executable, child.state)
            for event_id in incorporated:
                db.execute("""UPDATE wake_inputs SET incorporated = 1
                    WHERE session_id = ? AND event_id = ?""",
                           (activation.session_id, event_id))
            for publication in publish:
                self._append(db, publication.session_id, publication.event)
            if complete and db.execute("""SELECT 1 FROM wake_inputs
                    WHERE session_id = ? AND incorporated = 0 LIMIT 1""",
                    (activation.session_id,)).fetchone():
                raise ValueError("cannot complete with unincorporated mail")
            db.execute("""UPDATE wake_sessions SET state = ?, status = ?, deadline = ?,
                completed_at = ?, token = NULL, lease_until = NULL,
                revision = revision + 1 WHERE id = ?""",
                (json.dumps(state), "complete" if complete else "waiting", deadline,
                 now if complete else None, activation.session_id))

    def purge(self, *, completed_before: float, limit: Optional[int] = None) -> int:
        """Remove old complete sessions with their retained inputs.

        Completion time is the commit clock reading. Rows completed before the
        column existed have no completion time and are kept. Removal is
        atomic per call; a purged ID may be created again afterwards.
        """
        if not math.isfinite(completed_before):
            raise ValueError("completed_before must be a finite Unix time")
        if limit is not None and (type(limit) is not int or limit <= 0):
            raise ValueError("limit must be a positive integer")
        with self._transaction() as db:
            ids = [row[0] for row in db.execute("""SELECT id FROM wake_sessions
                WHERE status = 'complete' AND completed_at IS NOT NULL AND completed_at < ?
                ORDER BY completed_at, rowid LIMIT ?""",
                (completed_before, -1 if limit is None else limit))]
            for session_id in ids:
                db.execute("DELETE FROM wake_inputs WHERE session_id = ?", (session_id,))
                db.execute("DELETE FROM wake_sessions WHERE id = ?", (session_id,))
            return len(ids)
