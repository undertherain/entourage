# Deployment shards and automatic launch

2026-09-22. User-directed refinement of the automatic-launch work: independently
operated agent groups, with a runner for each group and residency selected per
agent. The primitives and manifest below are a proposal, not implemented APIs.
Current code remains the [local Python dispatcher](resumable-executables.md).

## Ownership boundary

A **shard** is a named execution domain that a deployment owns: its registered
executables, session/mail namespace, durable store, process policies and capacity
budget. Second Brain can be one shard containing Concierge, InternetResearch and
Events. Other applications can run their own shards without a global Entourage
runner. This is initially an explicit deployment partition; no automatic hashing,
rebalancing or distributed cluster membership is required.

A **runner** is the replaceable process serving that shard. It finds durable ready
work, launches or reuses a process, delivers an activation, commits the proposal
and applies the residency policy. It supervises several agent processes; it does
not synchronously run their handlers in its own scheduling loop. Killing one
agent must not prevent the runner from scheduling the others.

The deployment starts and supervises its runner using the host's process/service
lifecycle. A runner restart reconstructs ready sessions from the durable store.
Mail accepted into that store while the runner is down remains pending. An ingress
adapter that is also down still needs transport-side buffering or replay; the
session store cannot preserve a message that never reached it.

Separate shards reduce the scope of a runner failure. They do not provide high
availability within a shard or recovery from loss of its storage host. Start with
one supervised runner per shard. A later replica of that runner serves the same
logical shard; it is not another shard.

## Author-facing primitives

| Primitive | Responsibility |
| --- | --- |
| `Shard` | Group executable bindings, store, bootstrap sessions, service bindings and resource budgets |
| `Runner` | Recover ready work and manage activation/process lifecycles for that shard |
| `Executable` | Existing versioned code/config and resume contract; add a process-launch adapter |
| `Session` | Existing durable state, inbox, pending request identities and definition binding |
| `ResidencyPolicy` | Decide when a session process starts and what happens after a committed wait |
| `ServiceBinding` | Resolve an already-running external capability such as a NAS OCR endpoint |

The shard manifest composes existing executable manifests. The optional phase
router stays inside `agent.py`. Application grouping and process policy do not
require an execution graph or a separate handler for every internal action.

Proposed shape, deliberately independent of actual Second Brain paths or config:

```yaml
shard: second-brain
store: ./state/sessions.db

executables:
  concierge:
    manifest: ./concierge/agent.yaml
    residency: {start: eager, idle: resident}
  internet-research:
    manifest: ./internet-research/agent.yaml
    residency: {start: on_demand, idle: grace, seconds: 600}
  events:
    manifest: ./events/agent.yaml
    residency: {start: on_demand, idle: grace, seconds: 600}

sessions:
  concierge-main:
    executable: concierge
    initial_state: {phase: ready}

services:
  ocr:
    ownership: external
    endpoint: ${OCR_ENDPOINT}
```

Names under `executables` are deployment aliases resolving to the versioned
definition in each manifest. Residency defaults apply to processes for sessions
bound to that member. They are deployment policy, so changing a ten-minute grace
period does not require migrating application state or changing the code version.
Future per-session overrides must remain within the shard's resource limits.

`SessionBackend.list_sessions` supplies the enumeration eager start needs: known
nonterminal sessions per definition, without claiming them. Bootstrap creates
declared sessions only when absent. Restart never overwrites
saved state with `initial_state`, rebinds an existing definition silently or
resurrects a terminal session. New conversation sessions may instead be created by
application ingress. `start: eager` applies to known, nonterminal sessions; it does
not invent one conversation per definition or reserve an unspecified worker pool.
An initialized parked session may have its process warmed without creating a fake
mail event or an extra business activation.

First implementation: one session per agent process, potentially many processes
per shard. Loading a definition does not grant it one shared mutable conversation.
Pooling loaded models across sessions is a separate optimization; expensive shared
models can already live behind an external service binding.

## Residency and session continuity

Startup and idle behavior are separate choices:

| Choice | Behavior |
| --- | --- |
| `start: eager` | Ensure a process is ready for each known nonterminal session when the runner starts; replace crashed processes with restart backoff |
| `start: on_demand` | Launch when durable work is ready, including a new session's initial activation |
| `idle: resident` | Retain the process after parking; it holds no session write lease while idle |
| `idle: grace, seconds: 600` | Retain it for ten idle minutes after its last accepted waiting checkpoint |
| `idle: release` | Release it immediately after the waiting checkpoint is accepted |

For the first surface, pair eager startup with resident idle behavior. Cold or
grace workers use on-demand startup. Additional combinations can wait until their
restart and capacity semantics are defined.

An Events session can publish an answer, save phase `ready_for_followup` and park.
Its process then remains warm for ten minutes. New mail starts a fresh leased
activation using authoritative committed state. Once that activation parks, the
idle interval starts again. On grace expiry, release the process and retain the
session, mailbox and outstanding requests. A later question starts a fresh
process and restores that same session.

There are three independent boundaries: a response can be finished, the process
can be released, and the session can be terminally completed. The current
registered fixture uses `complete=True` after its answer, so it cannot serve as
the follow-up example unchanged. A long-lived agent must park after an answer;
terminal completion closes its mailbox to new distinct inputs. Publication keys
must also advance per logical response/request, rather than reuse the fixture's
single `answer` key across follow-ups.

The grace interval starts at accepted checkpoint time, not UI delivery time.
Keeping a process warm for ten minutes after confirmed user delivery would require
a delivery receipt and an explicit policy for it. The first implementation uses
checkpoint time. Grace expiry is a process-resource timer, not a business deadline
event, session TTL or history-retention rule.

Resident processes consume memory and count against the shard's resident-process
budget, even while they hold no activation slot or session write lease. Resident
capacity must be admitted explicitly; a ten-minute grace period can be best-effort
under a documented eviction policy because cold restoration preserves semantics.
The first proof can use fixed capacity with room for every configured fixture.

## Services that should simply stay up

The NAS OCR endpoint is a service with its own lifecycle and potentially expensive
loaded models. Bind it by address; the application adapter invokes it as a tool.
It does not need to speak the resume/checkpoint protocol, and releasing a caller
must not stop it. An asynchronous adapter can return its result through correlated
mail without holding the caller's activation open.

Start with externally managed services, owned by their NAS deployment. A later
managed-service member could declare start command, readiness check, restart and
shutdown policy under the same shard. That would be a service lifecycle contract,
not an implicit conversion of an HTTP server into a resumable session. Keep one
clear supervisor owner for each process. Service binding alone provides neither
durable remote delivery nor exactly-once effects.

## Isolation, ownership and shutdown

- Initially give each shard its own local SQLite file. Within a shard, state,
  incorporation and local publications retain the existing single transaction.
  Current claims filter by executable ID; registering the same definition in two
  runners against one shared file would not isolate their sessions. A shared
  physical database needs explicit shard scoping before it can replace this rule.
- Session addresses are local to their shard. Cross-shard mail needs a qualified
  route and a durable delivery adapter/outbox; it is outside the first proof.
- Enforce one live runner owner per local shard, including ownership of idle
  processes. Activation leases alone cannot prevent two runners from launching
  duplicate eager or warm processes. Replication would need further process
  ownership and failover rules, beyond the existing commit fencing.
- The host/runner supervision arrangement must retire orphaned child processes
  on runner failure before replacement ownership begins. A bare PID file is not
  sufficient identity. On orderly shutdown, stop assigning work, finish or
  terminate active children within a bound, and release owned processes. Durable
  sessions remain; any uncommitted activation retries after lease expiry.
- Serialize assigning a new activation with idle-process retirement. Mail arriving
  during grace expiry or shutdown remains durably ready and must trigger reuse
  or a fresh launch. An idle timer must never kill a newly assigned activation.
- Each activation still has its own hard execution limit and lease. Remaining
  warm does not extend an old lease. Logs, exit status and process health are
  separate from acknowledgment that the runtime accepted a checkpoint.

## Next implementation slice

1. Add a shard manifest/loader that composes executable registrations, local store,
   bootstrap sessions and per-member residency. Add a foreground shard runner
   entrypoint suitable for ordinary host supervision, without a global daemon.
2. Add the repeatable process activation/proposal/commit-acknowledgment protocol
   beneath that runner, retaining runtime-owned lease tokens and delivered IDs.
   Agent processes initialize once, then handle bounded activations independently.
3. Implement eager/resident, on-demand/release and on-demand/grace behavior with
   process ownership, hard activation timeout and bounded restart backoff.
4. Demonstrate Concierge remaining resident while Events answers, handles a warm
   follow-up, exits after grace and handles a cold follow-up from saved state.
   Use short/fake idle time in tests and 600 seconds in the illustrative policy.
5. Verify runner restart and orphan cleanup, crash before/after commit and lost
   commit acknowledgment, warm expiry races, unchanged bootstrap state, isolated
   shards sharing definition IDs, and another agent progressing during a hung
   activation. An external mock OCR service must survive runner/caller release.

Graphs remain optional. Reuse the process adapter for one existing graph node
after the shard proof, before building a separate graph worker manager. Automatic
child creation, replicated runners, shared storage failover, dynamic shard
placement and cross-shard messaging remain later work.
