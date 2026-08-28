# The coordination plane — Entourage as a consumer

Status: the plane contract itself — the three primitives, namespace and
resolver, plane client, spawn/child contract, monitors, ingress/egress
authority, distributed bindings — **moved to Aethera on 2026-08-18**: IOA
`docs/architecture/components/messaging/coordination-plane.md` (framing
decision: IOA ADR 0015 — messaging as the third protocol beside Vault and
Nexus). This document keeps what is Entourage's: how a Control-by-Return
graph consumes that plane. Sibling of
[`conversation-mailboxes.md`](conversation-mailboxes.md) (event model,
safe-point ingestion, retention); design journal in Second Brain
`_development_notes/entourage_message_board.md`. Mailbox storage
(`entourage.mailbox`), transition effects riding the atomic commit,
`WaitForMailbox` parking (`flow.WaitForMailbox` → a `waiting` execution
row; wake/timeout in `runtime/queue.py`), and transport-neutral ingress
(`entourage.ingress`) exist; spawn riding the commit is the remaining
design target here.

## What stays Entourage's

**No second execution runtime.** The plane never mentions the engine; the
engine consumes the plane through an adapter. Entourage owns coordination
*consumption* semantics: what an append means to a graph, turn and commit
boundaries, safe points, drain declarativeness.

**A subagent is another session** (2026-08-17). `Parallel` is scoped to
*within-session* fan-out — tool fan-out, tree-of-thought, deterministic
helpers with graph-native fan-in — and keeps its existing meaning for
workflow-style automations. Cross-entity composition happens only on the
plane; there is no cross-entity combinator (the plane has no workflow
algebra — see the IOA contract's settled framing). Retry within a session is
policy (`max_attempts`, mechanical, engine-owned); retry across sessions is
supervision, recorded by the parent at a safe point after a death notice.

**Two waiting modes** (2026-08-18): in fork-join, waiting is an *edge* — a
narrow `WaitForMailbox(match=correlation)` whose continuation was committed
at fork time; in supervision, waiting is a *turn* — a resident loop parked
on a broad `WaitForMailbox`, re-deciding at every wake whether to wait,
redirect, cancel, or pick up other work. Both are expressible in CbR; the
supervisor loop is the default for cross-entity work, the narrow join
reserved for a step that cannot proceed without the child's answer.

## The Transition surface

The plane's verbs, buffered and riding the atomic commit — no immediate
side-channel I/O from node code:

```python
def loop(state, events):              # drained events arrive as INPUT
    ...
    return Transition(
        state=new_state,
        plan=Sequence(tool, loop),    # or WaitForMailbox(...) | spawn + wait
        publish=[To("mailbox:diagnostics", kind="amendment", payload=...)],
        status="fetching logs, 2/5",
        # acknowledge: implicit — drained inputs auto-ack on commit
    )
```

There is deliberately no `ctx.mailbox.read()`. Draining is declarative:
events are a function argument, delivered at safe points. An imperative
mid-node read would break node purity and reopen the claim–persist–ack
problem the Transition commit closed. The interface makes the wrong thing
inexpressible.

## Spawn rides the commit

Spawn is a **Transition field** — an authoritative lifecycle act,
exactly-once via the commit. The child's entity id derives deterministically
from `transition_id + slot`, so crash replay re-spawns idempotently instead
of twinning the child. Lineage (`parent_task_id`, `correlation_id`, sponsor
authority) is recorded at spawn. The child's obligations (drain its mailbox,
optional status register, terminate with a correlated `kind: result`, be
covered by a monitor) are the plane's child contract — stated in plane
terms, so a child need not be an Entourage graph at all. The parent consumes
child results, death notices, and progress as ordinary drained events; a
blocking join is `WaitForMailbox` filtered on the spawn's correlation, and
recovery of the wait comes free from the graph store.

Implemented 2026-08-28: `Transition(spawn=[Spawn(plan, initial_state,
slot, correlation_id, notify)])` — the child session is staged
client-side (deterministic ids: `spawn-{exec_id}-{slot}` and onward) and
written inside `commit_transition` on all three stores; a child whose
session id already exists is skipped whole, so replay re-spawns
idempotently. Lineage rides the child's initial state under `"spawn"`.
The engine fulfils the child contract mechanically: a completing child's
correlated `kind: result` publication rides END's own transition commit
through the outbox (crash-safe replay); a failing child emits a
`kind: system` death notice as an idempotent append — and if that append
is lost in a crash, the armed monitor's lapse is the designed fallback.
Both observations feed monitors and wake parked parents. Fork-join is
`spawn` + a returned plan parking on `corr:{correlation_id}`; supervision
is `notify="<inbox>"` + a broad wait. Tests: `tests/test_spawn.py`.

## The four-verb adapter

What Entourage requires of any plane binding (in-memory and Redis today,
broker or Aethera messaging later, behind the injectable `mailbox_resolver`):

1. **idempotent `append`** by `event_id`;
2. **force-ack** keyed by consumer+event — the Transition commit, not the
   lease, proves incorporation;
3. **lease `claim`** for non-graph consumers sharing the same mailbox;
4. **wake subscription** — append (and monitor lapse, and eventually timer)
   makes a parked `WaitForMailbox` session runnable without duplicate
   activation.

Acceptance test for the seam: `append("mailbox:concierge", event)` never
changes when the binding moves Redis → broker → Aethera.

## Decisions 2026-08-28 — waits, timeouts, monitors, demux

Settled in a design session (journal: Second Brain
`_development_notes/entourage_message_board.md`); these resolve former open
questions 2 and 4 below and ingress questions 1–5.

**One journal, two compiled views.** The mailbox as a *runtime entity* is
one journal per session/agent — one address, one retention policy, one wake
subscription. The mailbox as a *programming abstraction* never exposes
filters. Two views compile onto the one journal:

1. **Request-reply, transparent.** A node calls a remote tool; the runtime
   allocates the correlation id, persists the completion route, and parks
   the execution. `match=correlation` is the *compilation target*, internal
   to the engine — never written by node code. The reply is delivered as
   the tool's return value (a timeout as its error); the next node runs
   as if the call had been inline.
2. **The drain: whatever's left.** The safe-point `events` argument
   receives the residual — everything no live route claimed.

Demux happens **at ingress, not at drain**: the ingress router resolves
correlation on arrival; a matching event goes to the parked execution, a
non-matching (or route-expired, late) event falls through to the residual
drain as ordinary/ambient. "Left" means *unacknowledged and unclaimed* —
the ack-on-Transition-commit ledger guarantees exactly-once incorporation
across both views. Consequence for the filter surface: correlation-only,
compiler-internal; the only user-visible wait is the broad supervisor form
`WaitForMailbox(timeout=…)` with no match argument. Parking cannot become
a query language because there is no user-facing query surface at all.

**A wait is an execution row.** The "non-runnable execution state vs
compiled `WaitForMailbox`" dichotomy is false — collapse it. A wait is an
execution in state `waiting` carrying a durable wake condition;
ready-detection extends from "all parents completed" to "all parents
completed and wake condition satisfied." Fan-in sees an ordinary
non-terminal execution; recovery of the wait comes from the graph store;
no new session or graph injection when the result arrives. Duplicate wakes
are harmless — execution idempotency (skip non-pending) already holds.

**Three timeouts, three owners.** "Timeout" is three different things:

- **Impatience** — belongs to the *waiter*; exists only where something
  waits. `WaitForMailbox(timeout=T)` stores T on the parked execution row;
  expiry delivers a `kind: system` timer event and the node *re-decides*
  (keep waiting, escalate, cancel). Erlang's `receive … after`.
- **Liveness** — belongs to a *monitor*, the "waiter" of the detached
  case. A durable expectation armed in the same Transition/outbox commit
  as the dispatch; on lapse it *appends* a `kind: system` event to the
  parent's inbox. Erlang's monitor/`DOWN`. The remote's own promised
  deadline informs T but never enforces it — a dead child can't report
  itself dead.
- **Staleness** — belongs to the *route*. `CompletionRoute.expires_at`
  bounds how long a late result is still routable; after expiry it lands
  in the residual drain as ambient, waking nothing.

All three ride one mechanism: persisted `(fire_at, action)` rows released
by a scheduler tick (the KIP outbox-release pattern), whose action is
always *append an event or make an execution runnable* — never a callback
into node code. The mailbox stays passive; the scheduler is the only
component that owns wall-clock time. **Cancellation is lazy**: timers are
not cancelled on result arrival; the tick checks route/wait state at fire
time and no-ops if the correlation was consumed.

**Monitors are declared expectations.** Two parameterizations of one
primitive: *deadline* (one-shot — expect `kind: result` for a correlation
by T; consumed by the matching append) and *heartbeat* (sliding — expect
activity from a source every ΔT; re-armed by matching appends, evaluated
lazily at fire time). Both cover a one-off remote tool and a resident
subagent identically, because in detach mode the parent's view of both is
the same: an external entity whose events arrive at the main mailbox. The
difference is expectation profile, not mechanism.

**Progress rides the register, not the mailbox.** Periodic "still working"
updates go to the child's status register (latest-value, overwrite); the
heartbeat monitor watches *register freshness*, not mailbox traffic. The
mailbox receives only discrete events — result, death notice, monitor
lapse, notable amendments — so the supervisor's drain stays about things
that changed and "is it stale?" is a register-timestamp check, not journal
archaeology.

## Open questions (consumer-side)

1. Drain-policy declaration: which safe points are implicit around LLM/tool
   nodes and which must the entity manifest declare (journal Q3)?
2. ~~`WaitForMailbox(match=...)` filter surface~~ — resolved 2026-08-28:
   correlation-only and compiler-internal; the user-visible wait takes no
   match argument (see decisions above).
3. Register reads at safe points: the canonical node form is (state, drained
   events) → Transition, so a supervisor's non-blocking "check the
   children's status" has no pure entry point. Read-set declared on the
   wait ("wake me with a snapshot of these registers") vs registers as a
   third node input.
4. ~~Timer wake surface~~ — resolved 2026-08-28: `WaitForMailbox(timeout=…)`
   delivering a `kind: system` timer event; the three-timeout split and the
   shared scheduler mechanism are recorded in the decisions above.
   Wake-source semantics still belong to the plane (IOA contract, open
   question 5).

## Decision task: transport-neutral return ingress

**Status: proposed 2026-08-27; pick up in Entourage.** The motivating
vertical is Concierge invoking `claude-remote` over Astral, but the contract
must also admit HTTP callbacks, Redis streams, polling adapters, and local
test transports.

One listener/ingress mechanism should accept normalized, idempotent events
from any transport and route them by a durable correlation registration.
The destination determines the consumption semantics:

1. **Narrow join:** deliver the result to a parked execution in the original
   session. Its existing continuation and fan-in remain in the graph; the
   result only makes that wait runnable.
2. **Resident notification:** append the result to an agent/conversation
   inbox. A detached task has already returned an acknowledgement and ended
   the originating turn; its later result starts another safe-point turn.

These destinations may share storage and claim/ack machinery, but they are
not the same public abstraction. Likewise an Astral subject is a transport
queue, not an Entourage conversation mailbox. Suggested vocabulary:
`TransportAdapter` (Astral/webhook/poller), `IngressRouter` (normalize,
deduplicate, resolve), `CompletionRoute` (correlation to destination), and
distinct execution-wait and agent-inbox sinks.

Dispatch ordering must close the fast-response race: allocate a stable
correlation id, persist its destination and the outbound publication in the
same Transition/outbox commit, then let the transport publish. Ingress must
persist delivery to the resolved sink before acknowledging the transport.
Replay reuses stable command and result event ids.

### Questions to settle before implementation — answered 2026-08-28

1. ~~Non-runnable execution state or `WaitForMailbox(match=correlation)`?~~
   Both — the dichotomy collapses: the wait *is* an execution row in state
   `waiting` with a durable wake condition; the narrow match is the
   compiler-internal form of that condition (decisions section above). The
   invariant holds by construction.
2. ~~Smallest transport-neutral port?~~ Publish plus a normalized
   inbound-event callback. Claim/ack/wake belong to the mailbox contract,
   which already exists; the ingress router terminates a transport event as
   an idempotent mailbox `append` (adapter verb 1) or a wake delivery, and
   acknowledges the transport only after durable persist. Reply subjects,
   webhook URLs, auth stay inside adapters.
3. ~~Where are correlation routes stored?~~ As durable `CompletionRoute`
   rows, registered in the same Transition/outbox commit as the outbound
   publication (closing the fast-response race). Await-routes target the
   parked execution; detach-routes target an agent/conversation inbox.
   Retention: `expires_at` per route; late results fall to the residual
   drain as ambient (staleness timeout, decisions above); no cancellation
   protocol — lazy fire-time checks.
4. ~~Await vs detach selection?~~ The caller's declaration at the tool
   surface, mapping one-to-one onto the two waiting modes; default detach
   for cross-entity work (the supervisor loop is the settled default),
   await reserved for a step that cannot proceed without the answer.
5. ~~Exactly-once completion across crashes?~~ The transition-commit
   pattern mirrored at ingress: persist the event and mark the wait
   satisfied in one commit, then enqueue the ready pointer. Duplicate wakes
   are harmless (execution idempotency); duplicate deliveries dedupe on
   `event_id`.

### Implementation state (2026-08-28)

Landed: `flow.WaitForMailbox(conversation=None, timeout=None, limit=20)`
compiles to a `__WAIT__` execution whose wait parameters ride the policy
field; the engine drains claimable events into the successor's state under
`"events"` (acks riding the transition commit), completes with a
`kind: system` timer event past the deadline, or parks the row as
`waiting` with a durable wake condition. `wake_due_waits()` is the
transport-neutral wake tick (poll cadence + fast path after publications);
waking is an atomic waiting→pending flip, so racing wakers are absorbed by
the pending-only idempotence guard, and a parked wait survives restart in
the graph store. `entourage.ingress` provides `InboundEvent`,
`IngressRouter` (resolution: explicit route → live `corr:{id}` waiter →
transport hint → default inbox), and route staleness with lazy expiry.
Tests: `tests/test_waiting.py`, `tests/test_ingress.py`, all three stores.

Monitors landed as `entourage.monitors` (in-memory and Redis backends,
selected by the runtime resource family): one `Monitor` primitive with the
two parameterizations — deadline (one-shot, satisfied and removed by a
matching observation) and heartbeat (sliding window, refreshed by
observations, one lapse per quiet spell with a `cycles`-distinguished
idempotency key). Arming is a Transition effect (`arm=[Monitor(...)]`,
`disarm=[...]`) riding the same commit as the dispatch publication;
observation happens where events already flow (outbox publication and
`IngressRouter.accept`); lapse evaluation is lazy in the engine tick and
delivered as `kind: system`/`source: monitor` mail to the `notify`
conversation — waking a parked supervisor like any other append. Tests:
`tests/test_monitors.py`.

Spawn landed too — see "Spawn rides the commit" above for what was
implemented against the design.

Still open on this task: riding detach-route registration on the
Transition commit (today the dispatching tool registers before the
transport publishes — idempotent, orphan routes expire); durable
`RouteStore` and scheduler-tick timers beyond poll cadence; the status
register (heartbeat monitors currently watch event flow, not register
freshness); supervision *retry* policy (re-spawning a dead child is a
parent decision at a safe point, not engine magic — needs a worked
example once Concierge migrates).

### Acceptance vertical

- Concierge calls a typed `claude-remote` tool without importing Astral.
- Online `list`/`start` request-reply can use a narrow join.
- A deliberately delayed result survives worker restart and resumes the
  original fan-in exactly once.
- A detached operation returns a task id immediately; its later result enters
  the configured Concierge conversation as a new event.
- Replacing the Astral adapter with a fake HTTP-callback adapter changes no
  graph, tool, correlation, or inbox semantics.

Plane-side questions (child cancellation effects, register keying, boards,
approval gates, wake sources, child profiles) live in the IOA contract.
