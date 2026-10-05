# Mailbox-first scheduling — merging the graph runtime and the session core

2026-10-04 design discussion, **decided 2026-10-05**. Collapses Entourage's two
scheduling implementations into one: the session core. The *Decisions* and
*Migration* sections below are the build plan; [NOW.md](../NOW.md) tracks
progress. Read [resumable executables](resumable-executables.md) for the
session-core API this builds on.

## The problem

Entourage currently carries two schedulers that both decide "what runs next":

- **Graph runtime** ("Entourage 1"): Control-by-Return. A node returns a plan
  (`Sequence`/`Parallel`/`Conditional`); `QueueRuntime` splices it into a
  persisted execution graph; readiness is "all parents completed" (plus a wake
  condition since `WaitForMailbox`); fan-in merges branch states. The
  continuation is **runtime-owned data**.
- **Session core** ("Entourage 2"): `LocalSessions`, `Dispatcher`, shards. A
  session is durable state + its own mailbox + a lease; readiness is "has mail or
  a passed deadline"; a handler `(context, state, mail) -> proposal` decides. The
  continuation is **application state** (e.g. `state["phase"]`).

Goal: one scheduler, to reduce what we maintain, without losing the original CbR
selling points (dynamic plans from cheap nodes instead of one big planning LLM;
only the running step in memory; visible/replayable plans).

## Reframes that came out of the discussion

1. **The real difference is who holds the continuation, not granularity.**
   Steering absorption ("skip the hotel") needs a continuation the agent can
   re-decide at a wake. A continuation frozen at fork time cannot absorb it. The
   "coarse grain" of mailbox agents is a consequence of per-wake overhead, not a
   requirement.
2. **Graph scheduling is a special case of mailboxes.** Returning
   `Sequence(Generate, Send)` is sending to a successor whose address is
   single-use, created at commit, and shares the session's state and lifetime.
   A join is an `all`-wait over such addresses. The graph's `ReadyQueue` is
   itself one anonymous mailbox per namespace (any worker takes any ready
   execution; it does not route by node type).
3. **So mailboxes are never less expressive; the question is cost and
   readability.** Where the graph is better: (a) seeing what is scheduled next,
   (b) cheap same-worker hops, (c) free fan-in with state merge. All three are
   *within-session* concerns, not arguments for graph scheduling between
   entities.
4. **Use a mailbox when the successor has its own identity; use a plan step when
   it is "the rest of this computation".** Identity = a different conversation,
   agent, worker pool, lifetime, or delivery channel.
5. **Joins belong on the wait, not on the mailbox.** The mailbox stays one
   journal per session (settled 2026-08-28: no user-visible filter/query
   surface). A parked activation's wake condition has two independent knobs:

   | Knob | Values |
   |---|---|
   | Completion: which awaited exchanges must resolve | `any`, `all`, `k-of-n`, deadline |
   | Interruptibility: does unrelated mail also wake me? | yes (agents) / no (strict workflow) |

   Graph fan-in = `all`, non-interruptible. Supervisor turn = `any`,
   interruptible. "Strict join that still accepts steering" = `all`,
   interruptible.

## Converged model

**The session core is the only scheduler.** Readiness, claim, lease, commit,
attempt limits, shards, upgrades and lifetimes exist once.

**Control-by-Return becomes the resume helper, not a runtime.** The handler-level
phase table in [resumable executables](resumable-executables.md#the-mini-router-belongs-inside-the-executable)
is a hand-written continuation (`state["phase"]`). A CbR plan is the same thing,
generalized:

| | Phase table | CbR plan helper |
|---|---|---|
| Continuation | a phase name in state | the remaining plan in state |
| Next-step set | fixed at authoring time | returned by nodes at runtime |
| Splicing ("X after me") | no | yes |
| Fan-out | hand-written | `Parallel` → in-process concurrency, or children + join |

The helper is a pure library: `(state, mail) -> proposal` that ingests mail,
advances a cursor over the stored plan, runs the next node, splices what it
returns, and either continues or parks. The runtime never inspects the plan; to
the runtime it is just state. The `flow.py` combinators remain the authoring API.

**A session is not a graph.** It is state + mailbox + lease. The remaining plan
`[X, B]` is a value in state, like `phase`. No execution rows, edges or ready
detection at the runtime level.

### Definition / session / worker

The confusion "one script = one mailbox, so A cannot call X in the same script"
dissolves once these are separated:

| | What | Cardinality |
|---|---|---|
| Definition | Python code + version (`research:v1`) | one per script/version |
| Session | durable state + **its own mailbox**, bound to a definition | many per definition |
| Worker | a process running a `Dispatcher` | serves all sessions of its registered definitions |

A mailbox forces a **commit boundary**, not a process boundary.

### Call and return (A → X → B)

1. Session S runs A. A returns "X, then continue"; remaining plan is `[X, B]`.
2. If X is another identity: the helper stages `context.spawn(...)` (or targets an
   existing session) plus `context.request(..., reply_to=S)`, records the request
   in state, parks. One commit.
3. X runs (possibly the same worker, same file, even the same definition with a
   different starting phase) and `context.reply(...)` lands in S's mailbox.
4. S wakes; the helper matches the reply, folds X's result into state, advances
   the cursor, runs B.

- The reply returns to the **session**, not to B. X never knows what follows.
- `A, X, A` (agent with tools) is the plan `Sequence(X, A)`; same mechanism.
  Workflow vs agent is only what is written after X: the README thesis again.
- **Each session is one stack frame**: `reply_to` is the return address, the
  stored plan is the instruction pointer. Nested calls give a distributed stack
  (continuation-passing / actor style). Unlike a call stack, the caller is not
  blocked (it can take steering while parked) and frames are durable.
- Delegation ("X's result should go to *my* caller, I'm done") is `reply_to`
  forwarding (cf. Erlang `gen_server`). Rare, optional.
- If X is **not** another identity (same lifetime, same pool, fits in one lease,
  no steering needed mid-step), it is an inline plan step in A's activation: no
  spawn, no mailbox, no extra commit. The helper decides per step; A returns
  `Sequence(X, B)` either way.

**Semantic change:** across a mailbox, X is a function (input message → result)
rather than a mutator of the session's state. The session merges the result,
like a tool result.

### Parallel

The session core has one writer per session (one lease). So `Parallel` is either:

- **in-process concurrency within one activation** (asyncio/threads): enough for
  the common case, I/O-bound LLM and tool fan-out. A crash reruns all branches of
  that activation; or
- **child sessions** whose results return as mail, joined by the exchange
  counter: for branches needing other workers or exceeding the lease.

*Retracted:* "branch activations" (branches running on other workers inside the
parent session, proposing results without writing state). That would be a second
scheduler hidden inside the session.

## Worked example: Telegram group manager

Today one per-message graph session runs `Triage → Generate → Send`, and
conversation continuity lives outside the graph (`ContinuousConversation`,
`ChatHistory`). The stages have **different identities**: triage is per event and
stateless; generation is per conversation, needs history, coalesces messages
arriving mid-work, absorbs steering.

Mailbox-first (built 2026-10-05, both triage shapes in the example):

- **Conversation agent**: per-conversation session holding the typed history;
  turns are two checkpointed phases (triage, answer) so mail arriving during
  triage joins the answer. Interruptible by construction.
- **Triage**: either the first phase of that turn (one call per batch, sees the
  conversation) or a per-event session that labels and forwards each message
  (parallel, stateless, no triage model in the chat). Both deliver every event
  to the conversation: the chat needs chatter as context, so the split labels
  rather than filters.
- **Send**: a committed publication to a Telegram outbox session, delivered by
  its handler (at-least-once).
- "Insert approval before Send" becomes address rerouting
  (outbox → approval → outbox) instead of a plan splice, and works across
  sessions.

Readability is kept per level: within a session, the persisted plan; between
sessions, the routing (which addresses a session publishes to), which is static
and drawable, n8n-like, at the level where it is actually stable.

## Resources

- **Low footprint is preserved.** A parked session holds no memory; an activation
  loads state + mail, runs, commits, releases. The same trampoline, bounced
  through an addressed durable queue. Difference: an activation loads the whole
  session state, not one node's slice. Keep session state small; reference bulky
  data.
- **Heterogeneous allocation is a session-core property.** Per-address pools,
  reserved capacity, idle grace and per-agent images ([deployment shards](runner-shards.md)),
  e.g. a cheap triage pool and a strong-model conversation pool. The graph's
  single ready queue cannot do this today.
- **Cost:** every identity-crossing hop is a checkpoint + claim. Hops that stay
  inside a session do not touch a queue.

## Decisions (2026-10-05)

Reviewed against the code after the discussion above. These narrow the
converged model to what gets built.

**The whole lifecycle is one shape.** Creation is the first checkpoint, every
wake goes through the same `resume(context, state, mail)`, completion is the
last checkpoint. There is no "resume from wait" special case and no wait node:
every proposal that is not `complete` parks. The pre-injected initial graph of
Entourage 1 becomes the initial state (`{"phase": "triage"}` or a stored plan
list); "what's next" is the handler reading that continuation.

**Three cross-session relations, two in-session instruments.** Between
sessions: fire-and-forget `send` to an identity-keyed address (Telegram triage
to conversation); `request`/`reply` to an owned, spawned child (subagent: the
parent's mailbox is the read end, the child's mailbox the steering end,
`reply_to` the return path, a UNIX pipe whose read end is deliberately shared
with user steering); `request`/`reply` to an existing service session. Within a
session: an inline call (plain Python) and a phase checkpoint. In-process
parallel is a third when needed. The graph algebra buys nothing within one
identity: `Sequence(A, B)` authored at the top is a phase table.

**No plan helper first.** For the three practical cases (fixed `A, B`, agent
`A, tool, A`, triage handoff) a phase variable and inline tool calls suffice.
The cursor-over-plan helper is built when a consumer needs runtime splicing
("X after me"); it is about twenty lines over a plan list in state. If it is
ever needed in full, it can be built from existing pure parts: `stage_plan`
in `runtime/planner.py` plus `InMemoryGraphStore` serialized into session
state, which would carry `test_graph_algebra.py` over as its specification.

**Exchanges and strictness are helper state, not backend features.** The
pending-exchange table lives in session state behind a small library helper
(record request, match reply, tell "my reply" from steering). A strict join
buffers unrelated mail in state and re-parks: one activation per stray
message, the backend stays always-interruptible with one claim path, and no
correlation surface is added to the mailbox (settled 2026-08-28). Pushed into
the backend only if wake churn becomes measurable.

**Commit per node is the default and needs no runtime change.** A commit ends
the activation and clears the lease, so "commit and keep running" is not
possible and not needed: `propose(state, deadline=now)` makes the session
claimable again at once. With no lease renewal, any LLM step is its own
activation anyway; batching inline is only for cheap deterministic steps.
Small addition: expose the lease deadline on `Context`. Tree-of-thought over
LLM branches therefore uses child sessions; in-process `Parallel` covers
tool fan-out.

**Gaps accepted, to be decided per step in the migration:**

- *Per-node policy.* `Node(max_attempts, timeout, retry_delay,
  max_invocations)` has no runtime equivalent; the session core has
  per-definition attempts and the lease as the only timeout. With one node per
  activation the attempt counter maps one-to-one; policy below the definition
  cap is handler code.
- *Replay.* The graph store kept every execution row; the session store
  overwrites state and bumps a revision. If visible history matters, the
  handler appends a compact trace or the backend grows optional per-commit
  snapshots. Not blocking.
- *Monitors and actors* are retired explicitly: deadline waits plus runner
  failure notices replace liveness monitors; singleton ingress keying plus
  eager start replace `register_actor`/`register_pipeline`.
- *Single host.* Retiring `QueueRuntime` drops Redis/SQS; the session core is
  SQLite until a Redis `SessionBackend` exists. Accepted: no current
  deployment needs more than one host; Astral is the distributed path.
- *Upgrades.* Persisted phase or plan names need the same `upgrades`/`migrate`
  handling as any saved state. No new mechanism.

## Migration

Each step is a commit with its own proof, ordered by what reuses what. The
backend does not change.

1. **Agent loop** (`A, tool, A`). *Done 2026-10-05.* `entourage/turn.py`
   replaces `AgentWithTools`/`PersistableAgent` with `ChatAgent.resume`: one
   activation per model call, tools inline, tool results checkpointed with
   `deadline=NOW` before the next call, messages in state, the answer as mail
   to an output address. `examples/cli.py` and `coding_agent.py` run on the
   `Dispatcher` with a printing `ui` session as the output adapter; the
   file-backed `ChatHistory` is no longer needed there. The graph `agent.py`
   stays until step 5. Tests: `tests/test_turn.py`.
2. **Telegram** (`A, B`). *Done 2026-10-05.* The old demo was already off
   the graph, running over `InMemoryMailbox` with sleeps standing in for
   checkpoints. Now `examples/telegram_group_manager.py` is one session per
   chat (`SessionIngress`, conversation keying) holding the typed history in
   state, with two checkpointed phases: triage on the cheap model, commit
   with `deadline=NOW`, then the answer, so mail that arrives during triage
   joins the answer's context. Replies and announcements are mail to a
   singleton `telegram-outbox` session whose handler calls the Bot API
   (at-least-once). Two triage shapes are kept side by side to see which
   sticks (`GROUP_MANAGER_TRIAGE`): *phase*, triage as the first phase of the
   chat's turn, one call per batch with the conversation as context; and
   *session*, a per-event triage session that labels each message
   `trigger: true|false` and forwards it, so the chat session never runs a
   triage model and answers when a batch holds a trigger. Every event reaches
   the chat either way, because the chat's state is the history; the split
   relabels, it does not filter. The split needs `SessionIngress.ensure` so
   the chat session exists before triage forwards to it. `deployment.py` and
   `config.py` were left alone; Second Brain imports them (see step 5).
   Tests: `tests/test_telegram_integration.py`.
3. **Pipe** (subagent). *Done 2026-10-05.* `entourage/exchanges.py` keeps the
   pending-exchange table in state: `request`, `call` (spawn plus request in
   one checkpoint), `ingest` (replies matched on request ID and expected
   sender, everything else returned as ordinary mail) and `drop` (forget a
   dead child). `runner.notify_failures` is the runner's death notice,
   callable on its own. `examples/spawn_supervisor.py` shows fork-join, an
   `all` join that folds a failure notice, and impatience via `deadline`;
   `examples/waiting_session.py` shows the three wake sources. Tests:
   `tests/test_exchanges.py`. NOW.md item 3 (two travel sessions) is covered
   by the parent-and-children test rather than a travel-themed demo.
4. **Sweep.** *Done 2026-10-05.* `examples/remote_tool_ingress.py`: the
   session is the return address, a late result is ambient mail after the
   deadline dropped the exchange; no ingress router is needed because
   `reply_to` names the session. `examples/retry_timeout.py`: per-definition
   `max_attempts` with backoff, a permanently failing step parked `failed`,
   and a step that outlives its lease losing its commit to the retry.
   `mailbox_cli.py` is not ported: the chat examples already coalesce
   interjections at checkpoints; it retires with `entourage.mailbox` in
   step 5. Monitors and actors are retired as decided above. Tests:
   `tests/test_session_examples.py`.
5. **Retire.** *Done in place 2026-10-05.* Because Second Brain imports
   `entourage.runtime`, `entourage.mailbox`, `capabilities`,
   `builtin_capabilities`, `config.load_agent_manifest`, `deployment`
   (`load_tools`, `import_object`), `runner.Shard`, `invocation` and
   `sessions`, the graph modules were not moved: `runtime/`, `flow`,
   `transition`, `mailbox`, `redis_mailbox`, `ingress`, `monitors`,
   `conversation` and `agent` now raise a `DeprecationWarning` on import and
   say so in their docstrings; the graph-bound classes in `deployment.py`
   and `config.py` are marked in the module docstrings while `load_tools`,
   `import_object` and `AgentManifest` stay current. Their tests keep
   running. `examples/mailbox_cli.py` is deleted, `python -m entourage` runs
   one question through the dispatcher, README and ARCHITECTURE describe the
   session model. The physical move to `legacy/` and the deletion happen
   once Second Brain's graph consumers are ported. `capabilities`,
   `invocation`, `runner` and `sessions` have no graph dependency and stay.

Deliberately skipped until a consumer asks: the plan helper, strict waits in
the backend, multi-host backends.

This subsumes NOW.md item 2 ("reuse the worker for one original graph node").

## Open questions

Resolved above: interruptibility default (on; strict is helper buffering),
plan nodes per activation (one per activation by default), in-session fast
path (in-process `Parallel` for tool fan-out, child sessions for LLM
branches). Still open:

- **Result merge** for mailbox-crossing steps and children: explicit fold in
  the joining handler for now; a default reducer only if a pattern emerges.
- **Replay** as above: trace in state vs backend snapshots. Decide when a
  debugging need appears.

## Related

- [Coordination plane](coordination-plane.md): two waiting modes, one journal /
  two compiled views, "a wait is an execution row", spawn on commit.
- [Resumable executables](resumable-executables.md): phase table, Context helpers,
  batches and failure.
- [Session lifetimes](session-lifetimes.md): ingress keying, spawned children.
- [Deployment shards](runner-shards.md): pools, residency, launchers.
- [Runtime handoff](execution-runtime-handoff.md): contracts to preserve,
  including "persisted state is not a Python stack".
