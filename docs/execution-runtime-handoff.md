# Shared execution runtime — implementation handoff

> **Status 2026-10-05:** the graph runtime this document describes is retired;
> the session core is the only scheduler. Kept as design history. See
> [mailbox-first scheduling](mailbox-first-scheduling.md).

2026-09-09. Entourage-owned implementation synthesis of the IOA discussion. This
file and [NOW.md](../NOW.md) track implementation here; IOA retains the cross-project
architecture decision and historical discussion. No production migration is claimed.

2026-09-22 implementation update: [registered resumable executables](resumable-executables.md)
now provide the local Python definition/dispatcher boundary and manifest. Input
batches and claim rotation are implemented. The commit review below describes
the earlier baseline; subprocess packaging, child creation and graph integration
remain follow-ups.

## Why this belongs here

Both original Control-by-Return graphs and mailbox sessions need registered code,
resource allocation, state restoration, commits and recovery. What makes work ready
and how continuations are represented differ; launching executables and managing
worker capacity should be shared. Extract useful existing transition/wait/outbox
semantics, without forcing mailbox sessions through graph internals.

EON source at `/home/blackbird/Projects/AI/Agents/DEMO/EON/eon` supplies the precedent:
`make_tool.py` adapts normal functions, `register_lambda.py` packages/deploys code,
`get_tool.py` invokes by name using synchronous RequestResponse. It is executable
deployment code, not only the sibling loop.py function-map example. No durable
session/wakeup protocol or Firecracker implementation was found in that package.

## Contracts to preserve

- Mailbox wait checkpoints and releases the activation lease; process exit is
  optional. Immediate release, resident and idle-grace policies share the same
  durable wake semantics. See [executable lifecycle](executable-lifecycle.md) for
  the packaging and protocol proposal from the 2026-09-09 design review.

- Definition = versioned code/config/launch descriptor. Session = durable state,
  mailbox, definition binding and pending exchanges. Activation = a leased attempt.
- A work queue chooses a worker; session mail first identifies the exact state to
  resume. Many sessions use the same code and worker pool. A stale worker cannot
  commit even if its external process continues after its lease expires.
- Persisted state is not a Python stack. Restart at a known entry point. SDK
  helpers can supply handlers, model history and pending-tool tracking; business
  phases or graph continuations still need explicit representation.
- Persist state, incorporated inputs, outgoing intents, child dispatch and wake
  conditions consistently. Execution proposes; the runtime commits. Retry-safe
  dispatch does not make external effects exactly-once; use idempotency/reconciliation.
- Parking an activation does not cancel the session's children. Cancellation and
  completion are separate operations. This differs from the IOA asyncio teaching
  demo's scope-exit cleanup and must survive any reuse of that example.
- Outstanding exchange, currently runnable activation and business dependency are
  distinct facts. A parent awaiting a booking still processes user amendments.
- Wake hints are optional optimizations. Persist/recheck readiness to avoid lost
  wakeups and recover it after restart. Deadline identity survives attempt retries.
- Progress observation needs an independent view/cursor, not claims competing with
  the session's inbox consumer. Principal enforcement is not implied by a reply
  handle or event-kind filter.

## Review of e325d84

The first local slice is aligned with the direction. LocalSessions is independent
of graph/model imports; file-backed SQLite supplies durable state and mail.
BEGIN IMMEDIATE serializes claims/appends/commits. Lease-token/expiry checks fence
commits. Pending mail remains runnable after parking. State, incorporation and
local publication share a transaction. Eleven focused tests passed on 2026-09-09.

Limits relevant to the next slice:

- `executable` is currently an opaque string, not a registered launch descriptor.
  wake_session.py manually selects and executes behavior; tick is caller-driven.
- LocalSessions and QueueRuntime remain separate scheduling/commit implementations.
  This is an acceptable first seam, not yet proof of the shared execution core.
  Demonstrate a graph node using the same launcher early to prevent divergence.
- Claim uses insertion-order selection and loads every pending event. A repeatedly
  ready early session can starve later ones; a large inbox can create a large
  activation. Documented correctness baseline, not worker-pool scheduling yet.
- There is no lease renewal. A slow activation loses commit authority and can be
  retried. The subprocess layer must define bounded execution/renewal/termination;
  fencing commits alone cannot undo an external tool's effect.
- Activation contains mutable data and commit validates incorporated IDs against
  that returned object. This is a trusted in-process interface today. An external
  executable must not supply its own authoritative input set or lease credentials;
  validate its proposal against runtime-held activation data.
- Local publication is atomic within one database. Remote publication requires an
  outbox, stable operation IDs and replay, not network calls inside the SQL commit.
- The demo prints a question after committing. This illustrates parking, but a
  crash between commit and print can lose the user-visible question. In the travel
  demo, record the question as a committed publication to a UI mailbox and let a
  separate output adapter deliver it with its stated delivery guarantees.

No blocker was found for the stated local wakeup scope. Missing features above
are next-stage contracts, not claims that the existing implementation has them.

## Acceptance example

Two trips share registered travel code. Each parent creates a tour child with an
explicit brief, automatic identity and scoped reply route. A child asks for a
preference; the parent publishes the question to the user and both activations
exit. Fresh processes restore state after the user answers, forward the answer,
receive a mock booking confirmation and update the itinerary. No real booking API.

Verify abrupt exit before/after commit, duplicate delivery, arrival while parking,
lease expiry and rejected stale commits, isolation of trips and replay-safe child
creation. Run one graph node through the same executable adapter. Keep SDK and
runner machinery outside the small author-facing examples.

## Source records

- IOA decision: `/home/blackbird/Projects/IOA/docs/architecture/decisions/0018-agent-runtime-above-astral.md`
- Discussion: `/home/blackbird/Projects/IOA/docs/architecture/memos/durable-sessions-and-project-home.md`
- Earlier handoff (contains superseded placement): `/home/blackbird/Projects/IOA/docs/architecture/memos/communication-first-agent-runtime.md`
- Teaching demos: `/home/blackbird/Projects/IOA/prototypes/mailbox_lab/README.md`

Do not copy IOA's entire NOW.md: its Fabric, Vault and product roadmap remains
owned there. This handoff brings only Entourage's execution work into this repo.
