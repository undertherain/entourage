# Session backend contract

Implemented 2026-09-22 in [`entourage.session_backend`](../entourage/session_backend.py).
`SessionBackend` is the abstract storage interface consumed by `Dispatcher`.
`LocalSessions` is its SQLite implementation. Redis and Astral adapters are not
implemented for this session core yet; existing graph-runtime Redis backends are
separate consumers and do not implement this interface.

## One checkpoint authority

The interface covers state, mail, activation leases and wake conditions together.
A backend instance represents one namespace and atomic commit domain. The runner
does not assemble a checkpoint by independently writing a state store and queue.

| Operation | Contract |
| --- | --- |
| `bind_definition(executable, contract)` | Persist a version's definition contract; identical rebinding succeeds, a changed contract is rejected |
| `create(session_id, executable, state)` | Create a ready session; never overwrite an existing one |
| `inspect(session_id)` | Observe saved state/status without claiming or consuming mail |
| `append(session_id, event)` | Persist mail idempotently by destination and event ID |
| `claim(lease_seconds=30, executable=None, max_events=None)` | Lease a ready session and return restored state plus an ordered input batch |
| `commit(activation, state, incorporated=..., publish=..., deadline=None, complete=False, spawn=...)` | Atomically save state, incorporate inputs, create spawned children, publish within this domain, set the next wake and release the lease |
| `purge(completed_before=unix_time, limit=None)` | Delete complete sessions older than the cutoff with their retained inputs; return the count |

All `claim` arguments are keyword-only. `commit` requires explicit incorporated
IDs; receipt alone does not acknowledge inputs. A finite deadline is Unix time;
omitting it clears the previous deadline. Empty mail can still accompany a new
session's initial activation. Pending mail, due deadlines and expired activations
must remain discoverable after restart, independently of notification delivery.
Backend configuration owns namespace selection and clock/connection setup.

Spawned children (`Spawn(session_id, executable, state)`) are created ready at
revision zero before publications, so one commit can create a child and mail
it. Each needs a distinct new ID and a bound definition; otherwise the whole
checkpoint rolls back. Children are independent afterwards: nothing about the
parent's later completion or purge propagates to them. Purge never touches
nonterminal sessions or sessions with an unknown completion time, and it ends
duplicate detection for the removed IDs. See [session lifetimes](session-lifetimes.md).

`Activation`, `Publication`, `Spawn`, `SessionSnapshot`, `StaleActivation` and
`SessionAlreadyExists` live with the interface. `Activation` is a runtime-held
snapshot with an opaque lease token, state, events and `has_more`; handlers receive
copies without lease credentials. `SessionSnapshot` describes the existing
dictionary returned by `inspect`: executable, state, status, deadline and revision.
The old shared-type imports from `entourage.sessions` remain supported.

## Errors and recovery

- `SessionAlreadyExists` means the identity is already present, including after
  completion. SQLite now reports this common error instead of leaking its duplicate
  key exception. Existing state, revision, definition and mail remain untouched.
  From `commit`, it means a spawned child already existed and nothing was saved.
- Unknown observation/publication destinations raise `KeyError`. Definition
  mismatches and invalid acknowledgments/wake conditions raise `ValueError`.
- An expired, replaced or already-committed activation raises `StaleActivation`.
  Repeating a successful commit cannot produce a second transition.
- Duplicate mail returns `False`; its first payload wins. Deduplication includes
  incorporated inputs and duplicate delivery after terminal completion. New
  distinct mail to a complete session is rejected.

Contract-validation failures must have no partial checkpoint effects: in
particular a bad publication cannot leave state changed or earlier publications
delivered. This is an observable requirement for every backend, regardless of how
its storage engine implements it.

Infrastructure failures can have an unknown outcome. A server could accept a
checkpoint and lose its connection before returning success. In that case
`DispatchResult.committed=False` means success was not confirmed by this call; it
does not prove the checkpoint was absent. Durable state, lease fencing and stable
publication IDs govern recovery. Each adapter must state its persistence and
acknowledgment guarantees. No backend makes direct external effects exactly-once.

## Selecting an implementation

```python
from pathlib import Path
from entourage.executables import Dispatcher
from entourage.session_backend import SessionBackend
from entourage.sessions import LocalSessions

backend: SessionBackend = LocalSessions(Path("state/sessions.db"))
worker = Dispatcher(backend)
```

No driver registry, backend-selection YAML or network dependency is required for
this extraction. A future Redis implementation must supply the same seven methods
and guarantees, including server-side validation/commit fencing, namespace
isolation, durable readiness and duplicate handling. The runner and executable
contract can remain unchanged. Automatic launch can proceed against SQLite.

Astral supplies cross-machine delivery and still requires a checkpoint store.
Bridging another commit domain needs durable incoming incorporation/outgoing
intents and replay; remote sends must not be slipped into `commit` as if they
participated in its local transaction. A transport adapter/outbox extension is
later work, alongside the [shard proposal](runner-shards.md).

## Conformance tests

`tests/test_session_backend.py` tests the public interface using the
`make_session_backend` fixture in `tests/conftest.py`. It contains the original
backend-independent lease/mail/checkpoint tests plus definition persistence,
duplicate creation, detached observations, bounded-batch recovery, namespace
isolation, atomic spawn with rollback, child independence and purge retention. Adding another adapter to that fixture runs the same assertions against
it. The fixture owns test namespaces and a controllable clock; those are not
public interface methods.

`tests/test_sessions.py` retains SQLite-specific subprocess/crash recovery tests;
`tests/test_executables.py` retains executable/dispatcher acceptance tests. Redis
will additionally need server restart, persistence and ambiguous-commit fault
tests beyond the shared behavioral suite.
