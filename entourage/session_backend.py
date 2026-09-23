"""Storage contract for graph-independent durable sessions.

One backend instance addresses one namespace and commit domain. State, mail
incorporation, publication and wake conditions must share its atomic checkpoint.
Connection setup, clocks, resource cleanup and physical storage are adapter-owned.
No graph, executable loader, database client or transport is imported here.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Literal, Optional, TypedDict


class StaleActivation(RuntimeError):
    """The activation no longer owns the session's write lease."""


class SessionAlreadyExists(ValueError):
    """Creation would replace an existing durable session."""


@dataclass(frozen=True)
class Activation:
    """Runtime-owned leased snapshot; token is opaque to executable code.

    State/events are detached from storage, but their nested values are mutable.
    The runtime must retain this object unchanged and give handlers copies.
    Tokens are valid only in the backend namespace that issued them.
    """

    session_id: str
    executable: str
    token: str
    state: dict
    events: list[dict]
    has_more: bool = False
    attempt: int = 1
    last_error: Optional[str] = None


@dataclass(frozen=True)
class Publication:
    """Mail to an existing session inside this backend's commit domain."""

    session_id: str
    event: dict


@dataclass(frozen=True)
class Spawn:
    """A child session created atomically with its parent's checkpoint."""

    session_id: str
    executable: str
    state: dict


Status = Literal["ready", "active", "waiting", "complete", "failed"]


class SessionSnapshot(TypedDict):
    """Detached observation of persisted state; reading does not consume mail."""

    executable: str
    state: dict
    status: Status
    deadline: Optional[float]
    revision: int
    attempts: int
    last_error: Optional[str]


class SessionListing(TypedDict):
    """One row of an enumeration: identity and lifecycle without state or mail."""

    session_id: str
    executable: str
    status: Status
    deadline: Optional[float]
    revision: int
    attempts: int
    worker: Optional[str]
    lease_until: Optional[float]


class SessionBackend(ABC):
    """Coherent persistence for sessions, mailboxes, leases and checkpoints.

    Inputs are JSON-compatible dictionaries with string keys. All operations are
    safe against concurrent callers within the same namespace. Durable readiness
    must survive reopening; notifications can optimize wakeup but cannot be its
    only source. A backend must document its persistence/acknowledgment guarantees.

    Validation failures and stale commits must have no partial checkpoint effects.
    Infrastructure failures may have an unknown outcome (e.g. a lost commit reply);
    recover from durable records and replay-safe event IDs rather than assuming
    every raised exception means nothing committed. Direct external side effects
    and cross-domain mail are outside this transaction contract.
    """

    @abstractmethod
    def bind_definition(self, executable: str, contract: dict) -> None:
        """Persist a version's JSON contract; identical rebinding is idempotent.

        Compare JSON content independently of object-key order. Raise ValueError
        on mismatch without replacing the existing contract. This registers data,
        not callable code, and must survive backend/runner restarts.
        """

    @abstractmethod
    def create(self, session_id: str, executable: str, state: dict) -> None:
        """Create a ready session at revision zero, even without initial mail.

        IDs must be nonempty. Raise SessionAlreadyExists if the session already
        exists, including after completion; never reset its state or definition.
        Definition registration is not required by this low-level operation.
        """

    @abstractmethod
    def inspect(self, session_id: str) -> SessionSnapshot:
        """Return a detached snapshot or raise KeyError for an unknown session.

        Do not claim work, consume inputs or increment revision. Status describes
        stored lifecycle, not computed readiness: waiting mail can be runnable.
        """

    @abstractmethod
    def list_sessions(self, *, executable: Optional[str] = None,
                      status: Optional[str] = None, ready: Optional[bool] = None,
                      limit: Optional[int] = None) -> list[SessionListing]:
        """Enumerate sessions in creation order, optionally filtered.

        Do not load state or mail, claim anything or change readiness. Complete
        and failed sessions are included unless filtered out. ready=True keeps
        only sessions a claim could take now; ready=False keeps the others.
        worker and lease_until describe the current holder of a claim, if any.
        A runner uses this to start workers for ready work, find stuck holders,
        find superseded definitions and reconcile after restart.
        """

    @abstractmethod
    def append(self, session_id: str, event: dict) -> bool:
        """Persist mail; return False for an already recorded event_id.

        Nonempty string event_id is required; the 'timer:' prefix is reserved.
        Deduplicate by destination and ID, including incorporated inputs and
        duplicates after completion. The first payload wins. New inputs to a
        complete session raise ValueError; unknown destinations raise KeyError.
        Mail is accepted while the session is active or waiting.
        """

    @abstractmethod
    def claim(self, *, lease_seconds: float = 30,
              executable: Optional[str] = None,
              max_events: Optional[int] = None,
              session_id: Optional[str] = None,
              worker: Optional[str] = None,
              max_attempts: Optional[int] = None) -> Optional[Activation]:
        """Atomically lease one ready session, or return None.

        New sessions, pending mail, due deadlines and expired attempts are ready.
        Honor the optional definition and session filters, exclude complete,
        failed and unexpired-lease sessions, and prevent starvation by rotating
        eligible sessions. No session may have two currently valid lease tokens.

        Every claim counts one attempt; a successful commit resets the count.
        The activation reports its attempt number and the error recorded by the
        previous release, if any. With max_attempts, a ready session whose count
        already reached it is moved to status 'failed' instead of being leased,
        durably and within this call, and the search continues. worker records
        who holds the lease, for operators and supervisors; it grants nothing.

        lease_seconds is finite and positive; max_events is a positive integer
        or None (unbounded). Deliver pending inputs in accepted append order.
        has_more reports queued inputs beyond this batch at claim time. A due
        Unix deadline becomes kind='system', source='timer' mail, with its
        deadline in payload. Its event ID must be stable across attempt retries.
        Readiness and timer identity cannot depend on process-local memory.
        """

    @abstractmethod
    def commit(self, activation: Activation, state: dict, *, incorporated: list[str],
               publish: tuple[Publication, ...] = (), deadline: Optional[float] = None,
               complete: bool = False, spawn: tuple[Spawn, ...] = (),
               rebind: Optional[str] = None) -> None:
        """Atomically save state, incorporate inputs, spawn, publish and set the wake.

        Validate against the runtime-held activation: incorporated IDs must have
        been delivered, and its token must still own an unexpired lease. Raise
        StaleActivation for expired/replaced/already-committed attempts. Validate
        all publication destinations and inputs without partial effects on error.

        Spawned children are created ready, at revision zero, before publications
        are delivered, so the same commit may publish to them. Each child needs a
        distinct new ID and an executable bound through bind_definition; raise
        SessionAlreadyExists or ValueError and roll back the whole checkpoint
        otherwise. Children are independent sessions afterwards: the parent's
        later completion, purge or failure does not cancel or complete them.

        rebind names a different bound definition the session belongs to from
        this checkpoint on; it cannot accompany complete=True. The saved state is
        the first state that definition sees. Unincorporated mail is retained and
        remains ready for the new definition. Session ID, revision history and
        retained input IDs are unchanged, so no publication is replayed.

        Success increments revision once, releases the lease and sets status to
        waiting or complete. deadline replaces the old deadline (None clears it);
        it must be finite and cannot accompany complete=True. Completion rejects
        any remaining mail, including arrivals after claim or self-publications.
        Unincorporated mail stays ready. Parking does not cancel outstanding work.
        A repeated commit with the same token is stale, not a second transition.
        """

    @abstractmethod
    def release(self, activation: Activation, *, error: Optional[str] = None,
                retry_after: Optional[float] = None) -> None:
        """Give up this attempt without a checkpoint; the attempt stays counted.

        Raise StaleActivation if the token no longer holds the lease. Record the
        error text for the next attempt and operators. With retry_after, the
        session is not claimable again for that many seconds; otherwise it is
        ready immediately. State, mail and revision are untouched.
        """

    @abstractmethod
    def retry(self, session_id: str) -> bool:
        """Return a failed session to service with a zero attempt count.

        Return False, changing nothing, when the session is not failed. Mail
        accepted while failed remains pending, so the session is typically ready
        at once. Raise KeyError for an unknown session.
        """

    @abstractmethod
    def purge(self, *, completed_before: float, limit: Optional[int] = None) -> int:
        """Delete complete sessions whose completion time is before the cutoff.

        Return how many sessions were removed, at most limit when given. Never
        touch ready, active or waiting sessions, and never a session whose
        completion time is unknown. Purging ends duplicate detection for the
        removed sessions: their IDs become unknown, so they may be created again
        and their retained input IDs no longer absorb replays. Retention policy
        must therefore outlast every transport's redelivery window.
        """
