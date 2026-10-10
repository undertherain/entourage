# NOW — Entourage execution runtime

Last updated: **2026-10-05**. Current implementation entry point. Read README.md
for the existing graph model, then this file. TODO.md retains earlier research
and publication history; its July “current direction” is not the current build plan.

## Agreed direction

**2026-10-05: the session core is the only scheduler.** Decided in
[mailbox-first scheduling](docs/mailbox-first-scheduling.md): the graph runtime
(`QueueRuntime`, `flow.py` plans, `WaitForMailbox`, graph spawn and monitors)
is retired in five steps, each a commit with its own proof. No backend changes:
exchanges and strict joins are handler-side state, one activation per node by
default, the plan helper is deferred until a consumer needs runtime splicing.
All five steps are done (2026-10-05): `entourage/turn.py`,
`entourage/exchanges.py`, `runner.notify_failures`, `SessionIngress.ensure`;
every example runs on the dispatcher (the Telegram demo carries both triage
shapes, phase and per-event session, to see which sticks); the graph modules
are deprecated in place because Second Brain still imports them. Remaining:
port Second Brain's graph consumers, then move the graph modules to `legacy/`
and delete them. The earlier
direction below stays as history; its item 2 is superseded.

Entourage owns a graph-independent execution core shared by graph workflows and
mailbox-driven sessions. Register launchable executables, wake bounded activations,
commit their transitions and release compute. Existing Python-callable graph nodes
remain supported. Do not make graph authoring mandatory for conversational agents.

Mailbox waiting ends an activation, but process exit is optional: support immediate
release, resident processes, and an idle grace period (for example 10 seconds).
Process residency does not retain a session write lease. Packaging/backend and
protocol proposals: [executable lifecycle review](docs/executable-lifecycle.md).

User refinement (2026-09-22, built 2026-09-23): a deployment owns a named **shard**
of cooperating agents and its own supervised runner, with no global runner dependency. Second
Brain can be one shard. Agent processes can be eager/resident or launched on
demand with release or idle grace (for example Events staying warm for ten minutes).
Service bindings let an independently managed NAS OCR endpoint stay running.
Proposed primitives and next proof: [deployment shards](docs/runner-shards.md).
Worker pools, priorities and reserved capacity are execution concerns within a
shard; replicated runners and dynamic sharding remain later work.
Simple local file-backed communication supports debugging; Astral is a future
binding for distributed delivery. Distributed delivery alone does not supply state
failover. Second Brain retains application tasks, conversation and knowledge policy.

**2026-10-10: work protocol drafted, not decided.** A durable handle as the
primitive between callers and providers; a session is most of that handle
already. Brainstormed the same day: a wire sketch (provider decides within the
caller's wait budget), pipes opened at runtime with capabilities checked like
types, a panel per agent (grown from `entourage.exchanges`), and case A (an LLM
loop calling subagents) worked through against `ChatAgent`. Next: prototype one
`ChatAgent` delegating to another through a pipe. All in
[work protocol](docs/work-protocol.md), with a discussion log. Its first
application, Library proposals, is Second Brain's.

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

**Backend interface extracted (2026-09-22):** `entourage.session_backend` defines
the six-method `SessionBackend` contract and shared activation/publication/snapshot
types. `LocalSessions` implements it; the dispatcher no longer imports a concrete
store. The shared contract suite is ready for another adapter, while SQLite
subprocess recovery tests remain separate. Focused validation: **36 passed**.
Full repository regression: **239 passed, 132 skipped**.
See [session backends](docs/session-backends.md). Redis remains unimplemented for
this core; automatic launch can build against the SQLite adapter.

**Shard runner and workers (2026-09-23):** the store is the protocol. A worker
(`python -m entourage.worker`) is the dispatcher over one agent folder; it
records its name on leases and decides its own exit after `idle_exit`. The
runner (`python -m entourage.runner shard.yaml`) reads the store and manages
processes through a process or podman launcher: eager and on-demand starts
with reserved and pooled capacity, backoff after quick exits, killing live
holders of expired leases, and failure notices as mail. The store counts
attempts per step, parks sessions as `failed` at `max_attempts`, and a failed
step is released at once with its error and backoff; `context.attempt` and
`context.last_error` reach the agent. No lease renewal: the lease is the hard
step limit. `Containerfile` builds the generic runtime image; agent folders are
mounted. See [deployment shards](docs/runner-shards.md). Focused validation:
**65 passed** across backend, executable, ingress, SQLite and runner suites.

**Session upgrades and enumeration (2026-09-22):** `commit(rebind=...)` moves a
session to another bound definition at its checkpoint, keeping ID, history,
dedup and pending mail. `Executable.upgrades` maps superseded definitions to a
`migrate(context, state, mail)` handler; the dispatcher claims those sessions
only when ready and rebinds at commit, so migration happens lazily at the next
wake, which is the only moment a definition matters. `list_sessions` enumerates
identity and lifecycle without state for eager start and reconciliation. Prompt
files remain part of the code identity; see [session upgrades](docs/session-upgrades.md).
Focused validation: **53 passed**.

**Session lifetimes (2026-09-22):** lifetime is keying plus completion, not a
runtime property. `entourage.session_ingress` derives session IDs per event, per
conversation or singleton and creates-if-absent before an idempotent append.
`Context.spawn` stages a child `<parent>:<key>` created atomically with the
parent's checkpoint, before publications; an existing child rolls the proposal
back. `SessionBackend.purge` removes old completed sessions and ends their
duplicate window. Children are independent of parent completion; a reply to a
completed parent fails at the child's commit and retries until the attempt-limit
item lands. See [session lifetimes](docs/session-lifetimes.md). Focused
validation: **47 passed** across the backend, executable, ingress and SQLite
suites, including the legacy-database upgrade with the new completion column.

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

Automatic launch is built as the [shard runner](docs/runner-shards.md). Next:
runner ownership lease, container resource limits from the manifest, the
Second Brain shard itself.

1. Extend the local versioned registration/handler contract into a serialized
   activation/result protocol and reproducible executable packaging. Keep the
   runtime-owned commit boundary and separate definition/session/activation IDs.
2. Done as worker processes sharing the store (no step protocol). *Superseded
   2026-10-05:* graph nodes are not reused; the graph runtime is retired
   ([migration](docs/mailbox-first-scheduling.md#migration)).
3. Done 2026-10-05: `Context.spawn` plus `entourage.exchanges` (pending
   exchanges in state, replies matched on ID and sender, failure notices
   folded into joins); `tests/test_exchanges.py` covers parent and children
   across activations with steering while waiting.
4. Reserved and pooled capacity exist per shard. Remaining: priorities and
   cross-member fairness beyond claim rotation.
5. Add Astral through a transactional outbox and replay-safe operation identities.
   Preserve working local and graph consumers; no second checkpoint transaction
   beside an existing graph commit for the same logical operation.

Detailed constraints and review: [runtime handoff](docs/execution-runtime-handoff.md).
Current code contract and demo: [durable sessions](docs/durable-sessions.md).

Runnable authoring examples: [tool calls through mailboxes](docs/mailbox-tool-examples.md)
show request/result wakeups and a tool clarification round trip across fresh
processes. Their dispatcher and pre-created addresses are example scaffolding,
not completion of the executable registration or child-spawn milestones.

The [mailbox examples folder](examples/mailboxes/README.md) starts with the
registered executable example. Earlier receive-loop and saved-step helpers remain
as teaching prototypes. The registered fixture is intentionally finite; continuing
after a final answer and managing per-agent idle grace belong to the shard proof.

Example audit: **27 affected tests passed**, plus the four standalone graph demos,
two CLI help commands and the direct wake/restart walkthrough. Clarified in-memory
graph examples, thread timeout limits and the Telegram demo's separate history/
delivery operations. State inspection now uses the backend API where applicable;
the registered CLI can inspect saved state without loading executable source.

## Open / not implemented

Runner ownership lease, lease renewal, memory/payload-byte limits, purge
scheduling, runner-driven upgrade of quiet eager sessions, adoptable prompt
resources outside the code identity, a Redis `SessionBackend` and Astral
delivery. Pending exchanges are application state behind `entourage.exchanges`,
by decision; the runtime keeps no registry. Graph integration is no longer a
goal: the graph runtime is retired. Attempt limits and backoff exist; a
dead-letter policy beyond the `failed` status and the runner's notice does not.
Handlers have an optional ordinary Python phase router; arbitrary coroutine
stacks are not persisted. Firecracker is a later execution backend candidate.
