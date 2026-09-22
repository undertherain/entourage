# NOW — Entourage execution runtime

Last updated: **2026-09-22**. Current implementation entry point. Read README.md
for the existing graph model, then this file. TODO.md retains earlier research
and publication history; its July “current direction” is not the current build plan.

## Agreed direction

Entourage owns a graph-independent execution core shared by graph workflows and
mailbox-driven sessions. Register launchable executables, wake bounded activations,
commit their transitions and release compute. Existing Python-callable graph nodes
remain supported. Do not make graph authoring mandatory for conversational agents.

Mailbox waiting ends an activation, but process exit is optional: support immediate
release, resident processes, and an idle grace period (for example 10 seconds).
Process residency does not retain a session write lease. Packaging/backend and
protocol proposals: [executable lifecycle review](docs/executable-lifecycle.md).

One logical runtime per system/deployment initially; eventual sharding is open.
Worker pools, priorities and reserved capacity are shared execution concerns.
Simple local file-backed communication supports debugging; Astral is a future
binding for distributed delivery. Distributed delivery alone does not supply state
failover. Second Brain retains application tasks, conversation and knowledge policy.

## Verified baseline

Reviewed commit **e325d84309a839d125b65a1896285da1bb907f4c** on 2026-09-09.
`entourage.sessions.LocalSessions` provides file-backed SQLite sessions with
explicit JSON state, opaque executable IDs, mail/deadline readiness, expiring
activation leases and atomic state/input/local-publication commits. Readiness is
reconstructed from durable records. Parking leaves logical children intact.

Validation: `PYTHONDONTWRITEBYTECODE=1 python3 -m pytest -q -p no:cacheprovider tests/test_sessions.py`
— **11 passed**. Covers fresh-process recovery, abrupt exits around commits,
stale leases, concurrent claims, mail during activation, timer identity, rollback,
deduplication and session isolation. This is a focused session review, not full
repository or distributed-runtime certification.

## Next

**Implemented local slice (2026-09-22):** `entourage.executables` now registers
versioned Python definitions from code or a manifest and dispatches restored
state/mail to a handler that returns a proposal. Runtime-owned activations keep
lease credentials and authoritative input IDs away from handlers. One-shot and
resident execution share the same commit path over `LocalSessions`.

The [registered example](examples/mailboxes/registered/README.md) demonstrates two
sessions sharing one definition, correction while a tool is pending, independent
completion and fresh-process result handling. Local request/reply helpers stage
correlated publications. Bounded event batches expose `has_more`; claims rotate
between sessions and the dispatcher rotates definitions. Definitions persist a
contract with entrypoint/resource fingerprints, schema and config. This is local
source checking, not a packaged dependency environment or subprocess protocol.

Focused validation: **26 passed** across `test_executables.py`, `test_sessions.py`,
`test_mailbox_authoring_examples.py` and `test_mailbox_tools_example.py`. Includes
abrupt exits before/after dispatcher commit, duplicate requests/results, stale
leases, forged acknowledgment rejection and split-batch correction handling.
Repository regression: **229 passed, 132 skipped** (`python3 -m pytest -q`).
An additional legacy-database upgrade test passed, verifying that an existing
parked session resumes with its state and revision preserved.

Current API and limits: [resumable executable contract](docs/resumable-executables.md).
Consumer acceptance: [Second Brain mailbox-agent TODO](TODO.md#next-consumer-state-resumable-mailbox-agents-2026-09-22).
Second Brain migration has not been performed. Broader milestones remain:

1. Extend the local versioned registration/handler contract into a serialized
   activation/result protocol and reproducible executable packaging. Keep the
   runtime-owned commit boundary and separate definition/session/activation IDs.
2. Add a subprocess execution adapter: registered argv/cwd, serialized input/output,
   bounded execution, error handling and lease-expiry behavior. Allow repeated
   activation/commit exchanges so resident and grace policies fit the same protocol.
   Process success
   alone must not mean checkpoint success. Reuse the launcher for one original
   graph node before building a separate worker-management system.
3. Add atomic child creation and durable exchange/reply handles. Demonstrate two
   travel sessions sharing code: delegate tour, ask user, persist and exit, answer,
   resume child, return mock confirmation. Names and routing should be automatic.
4. Establish shared scheduling admission and bounded batches. Then add fairness,
   priorities and reserved/borrowable capacity with explicit starvation policy.
5. Add Astral through a transactional outbox and replay-safe operation identities.
   Preserve working local and graph consumers; no second checkpoint transaction
   beside an existing graph commit for the same logical operation.

Detailed constraints and review: [runtime handoff](docs/execution-runtime-handoff.md).
Current code contract and demo: [durable sessions](docs/durable-sessions.md).

Runnable authoring examples: [tool calls through mailboxes](docs/mailbox-tool-examples.md)
show request/result wakeups and a tool clarification round trip across fresh
processes. Their dispatcher and pre-created addresses are example scaffolding,
not completion of the executable registration or child-spawn milestones.

The [mailbox examples folder](examples/mailboxes/README.md) now starts with a
small resident receive loop and a save/exit/resume agent. Their shared helpers
demonstrate staged calls and saved-step dispatch over LocalSessions; this is an
example authoring API, not yet a public SDK or process supervisor.

## Open / not implemented

Executable deployment, subprocess management, atomic child spawn, runtime-managed
exchange tracking, worker pools, lease renewal, hard execution/memory/payload-byte
limits, retention, graph integration and Astral delivery. Pending requests are
currently explicit application state. Exceptions retry after lease expiry with no
attempt limit; no terminal failure/dead-letter policy exists yet. Handlers have an
optional ordinary Python phase router; arbitrary coroutine stacks are not
persisted. Firecracker is a later execution backend candidate.
