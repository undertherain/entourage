# Entourage

**Entourage is a small Python framework for building LLM agents and workflows as one
thing.** Steps are pure functions that *return* a declarative plan instead of calling each
other; a runtime executes the plan against a persistent graph, so runs are durable,
resumable, and replayable. The pattern it's built on is called **Control-by-Return**.

> Status: research prototype / reference implementation — a vehicle for the idea, not a
> production framework. Expect rough edges and a small, deliberately minimal API.

Current execution work: [NOW.md](NOW.md) and the
[shared runtime handoff](docs/execution-runtime-handoff.md).

---

## The idea

LLM systems usually sit at one of two ends of a spectrum:

- **Workflows** — hand-wired graphs of steps (à la LangGraph, Airflow, Temporal). They are
  deterministic and durable, each step is easy to test, and you can route every step to the
  most appropriate (often cheapest) model. The cost is brittleness: a workflow can only do
  what it was wired to do.
- **Agents** — an LLM in a Reason–Act loop (à la LangChain, CrewAI, AutoGen). They handle
  novel inputs because the model picks the next action at runtime, but they are expensive,
  the orchestration lives inside one model's context window, and the whole run sits in a
  single process — a crash mid-tool loses it.

Most real systems live in between, and the only thing that really differs between the two
ends is **where the decision about the next step lives** — in the graph you drew, or inside
a model at runtime. Entourage makes that location a *return value*, and the two ends become
two configurations of the same machinery.

### How it works

A **node** is a pure function:

```python
node: state -> (new_state, plan)
```

A node never calls another node directly. Instead it returns a **plan**, built from three
combinators:

```python
Sequence(a, b, c)        # run a, then b, then c
Parallel(a, b, c)        # fork-join; resulting states are merged
Conditional(key, plan)   # run plan only if state[key] is truthy
```

Any leaf can carry an execution policy — retries, per-attempt timeout, retry delay:

```python
Sequence(fetch, Node(call_api, max_attempts=3, timeout=10, retry_delay=1), report)
```

The policy is stored on the execution itself, so every worker honors it; a node that
exhausts its attempts fails its session terminally (`examples/retry_timeout.py`).

The runtime splices the returned plan into a **persistent execution graph**, between the
current node and whatever was scheduled to follow it. Because the plan is data on disk — not
frames on a call stack — a run can be paused, persisted, resumed on another machine, retried,
and replayed. (This is *trampolined execution*: each step yields control back to a scheduler
instead of recursing through the host language's stack.)

Two things follow directly:

- **An agent is a workflow with a self-edge.** The whole Reason–Act loop is one line — the
  node schedules a tool, then schedules *itself* to inspect the result:

  ```python
  return context, Sequence(tool, my_node)
  ```

- **A workflow is an agent without LLM decisions** — a node whose plan happens to be
  hard-coded. So the choice is no longer "framework A vs framework B" but, per node:
  *who picks the next step — me, or the model?*

And two useful properties come for free:

- **Per-step model selection.** Each node embeds its own model and prompt, so you can mix
  cheap and expensive LLMs within one flow — a cheap classifier can gate an expensive
  reasoner. Cost and capability are decided per step, not globally.
- **Durable, replayable runs.** Persistence, retries, and time-travel debugging come from
  the runtime, because the control flow is just a persisted data structure. Runtime-grown
  structures like Tree-of-Thought fit the same primitive: a node returns `Parallel` over
  candidate branches and the thought tree *is* the execution graph.

### A worked example

A Telegram community-manager bot. Each incoming message starts a session with the initial
plan `Sequence(Triage, End)`:

- `Triage` runs a cheap yes/no LLM. Off-topic → it returns no plan and the session ends.
- On-topic → it returns `Sequence(Generate, Send)`, which the runtime splices into the
  graph. `Generate` is a tool-calling agent on a stronger LLM with RAG; `Send` posts the
  reply.

One small program demonstrates per-step model selection, a graph that grows at runtime
(triage decides the rest of the plan), and human-in-the-loop readiness: an approval node can
be inserted before `Send` without touching any other code.

---

## Install

Entourage is a Python package; install it from the repository root:

```bash
pip install -e .
```

Provide API keys via a `.env` file:

```bash
OPENAI_API_KEY=sk-...
TAVILY_API_KEY=tvly-...   # for the search-tool example
```

## Quick start

A CLI example runs a persistable agent with memory and search tools:

```bash
python3 examples/cli.py
```

- Interact with the agent in natural language.
- `/new` starts a fresh session (clears context, keeps long-term memory).
- `--debug` enables model/tool output and runtime logging:

```bash
python3 examples/cli.py --debug
```

More examples live in `examples/` (`telegram_group_manager.py`, `coding_agent.py`).

To see Codex-like interjections, run the in-memory mailbox checkpoint demo.
It uses LiteLLM with `gpt-5-nano` by default and inserts short artificial work
stages so there is time to enqueue another user message, `/subagent ...`, or
`/ambient ...`. Events drained before the model call are included in that same
answer:

```bash
python3 -m examples.mailbox_cli
```

Select another model or change the timing with `MAILBOX_DEMO_MODEL` and
`MAILBOX_DEMO_STEP_DELAY`.

The demo uses a redraw-safe message composer: background checkpoint output is
rendered above it without destroying a partially typed message.

### Continuous agents

An experimental graph-independent session core now supports local durable
mail/deadline wakeups, leased activations and atomic checkpoints. See
[`docs/durable-sessions.md`](docs/durable-sessions.md) for its current scope and
a runnable example that parks and resumes across fresh processes.
`entourage.executables` adds versioned Python registration, local manifests and a
resident dispatcher: handlers return checkpoint proposals from restored state and
mail. The [registered agent example](examples/mailboxes/registered/README.md)
handles a user correction while a tool is pending, then resumes after restart.
See the [executable contract](docs/resumable-executables.md). Subprocess launching
and integration with the graph runner remain follow-up work.
The dispatcher depends on a [session backend interface](docs/session-backends.md)
covering state, mail, leases and atomic checkpoints; SQLite is its current
implementation, with a reusable conformance suite for future adapters.
Session lifetime is an application decision, not a runtime one: ingress keying
(per event, per conversation or singleton), parent-spawned task sessions and
completed-session retention are described in
[session lifetimes](docs/session-lifetimes.md). Long-lived sessions move to a
new definition version lazily at their next wake through declared upgrades; see
[session upgrades](docs/session-upgrades.md). A deployment runs its agents as a
shard: worker processes or podman containers over one store, supervised by a
runner that reads the store; see [deployment shards](docs/runner-shards.md) and
the runnable [demo shard](examples/shard/shard.yaml).

Start with the [registered example](examples/mailboxes/registered/README.md) for
the current API. The other [mailbox examples](examples/mailboxes/README.md) retain
the earlier receive-loop and saved-step teaching prototypes, plus a longer
[tool clarification walkthrough](docs/mailbox-tool-examples.md).

`entourage.conversation` provides a configurable loop for an agent whose
conversation outlives any one incoming-message execution:

- `ConversationPolicy` retains the legacy topic-shift fields for direct callers and
  selects a manual reset command such as `/new`. Generic configured agents disable
  semantic topic detection; applications own that policy.
- `ContinuousConversation` owns the live segment and prompt rebuilding around durable
  `ChatHistory`. It can still accept the legacy optional `TopicMemory`, but does not
  require a semantic archive provider.
- `ContinuousAgent` supplies the main model/tool loop while the application
  supplies its tools and system-prompt builder.
- `TopicMemory` is a compatibility helper for applications that still want the original
  litellm-based detector and summarizer. `archive_record()` returns a structured result
  so callers never reconstruct its filenames; `archive()` retains the original string-ID
  return for compatibility. New applications should keep topic semantics outside the
  execution substrate.

### Capabilities

Behaviour composes rather than subclasses. A **capability** is a self-contained
unit — durable facts, topic tracking, a tool family — that an agent registers,
and `entourage.capabilities` splits them by what a hook is allowed to do:

- **Contributive** (`Capability`) — many per agent. `prompt_section()` and
  `tools()` merge in registration order. One capability cannot invalidate
  another, so composition is safe by construction.
- **Exclusive** (`ConversationLifecycle`) — a capability that *also* owns
  conversation history; at most one per agent. It decides when a segment is
  archived, reset, evicted, or projected. Two owners would fight over the same
  state and the symptom would surface far away, as context that drifts or
  duplicates, so a second one is rejected at construction.

```python
agent = ConfiguredAgent(manifest, "telegram:9", capabilities=[
    Facts(MemoryDB(state_dir / "memory.txt")),   # contributive
    MyTopicRouting(...),                          # exclusive: owns history
])
```

`ConfiguredAgent.default_capabilities()` is empty: a configured agent is its
manifest's prompt and tools and nothing else. Memory and history policy are
composed in by the application, or by overriding that method.

The lifecycle returns a `TurnPlan`, which separates two things usually
conflated:

- `history` **replaces the durable record** — a reset, an archive, an eviction.
- `view` is **what the model sees for this call only**, leaving the record
  intact. It is how an agent shrinks or reshapes a prompt — trimming,
  summarizing, dropping tool traffic — without losing the conversation.
- `handled` short-circuits the turn with a reply and no model call.

`builtin_capabilities` ships `Facts`, `RecentSummaries`, and
`TopicShiftLifecycle` — the historical behaviours, as replaceable units with no
privileged access. An agent that composes none of them is a supported
configuration.

This is logical conversation continuity over turn-level execution sessions.
For graph-native waiting, `flow.WaitForMailbox` is a plan leaf that parks
its execution durably — status `waiting`, holding no worker — and wakes
when its conversation has claimable events or its timeout fires (delivered
as a `kind: system` timer event); drained events join the successor's
state and are acknowledged inside the transition commit.
`entourage.ingress` routes normalized external results (webhook, broker,
poller — transport adapters stay outside) into the right conversation:
back into a parked await, or into a resident agent's inbox. Child
sessions spawn atomically on the transition commit
(`Transition(spawn=[Spawn(...)])`) and report back as correlated mail;
monitors (`Transition(arm=[Monitor(...)])`) turn silence into
`kind: system` mail. Runnable walkthroughs, no external services needed:

```bash
python examples/waiting_session.py     # park, wake on mail, wake on timer
python examples/remote_tool_ingress.py # remote call joins or falls to inbox
python examples/spawn_supervisor.py    # fork-join, supervisor loop, monitor lapse
```

The agreed event model,
safe-point ingestion semantics, and independent conversation/context/graph
retention policies are recorded in
[`docs/conversation-mailboxes.md`](docs/conversation-mailboxes.md). The
communication layer above it is Aethera's coordination plane (contract in
IOA `docs/architecture/components/messaging/coordination-plane.md`);
Entourage's consumer-side contract — the Transition surface, spawn riding
the commit, and the four-verb plane adapter — is
[`docs/coordination-plane.md`](docs/coordination-plane.md).

For multi-agent deployments, `RuntimeBackendConfig` selects one coherent family
of graph-store, ready-queue, and mailbox strategies. `backend: memory` gives a
zero-infrastructure test runtime; `backend: redis` derives three isolated
namespaces from an application-selected prefix. Agents can share one Redis
deployment while keeping scheduler namespaces isolated when their workers
register different node sets.

`entourage.deployment` removes the repeated worker ceremony from those
deployments. An application-owned YAML manifest selects the agent id, trigger,
models, prompt file, state directory, setup hook, and tool factories. Relative
paths resolve beside the manifest, so the same format can live in another
repository. `AgentWorker` turns it into a durable trigger pipeline and keeps
one continuous agent per `conversation_id`; a publisher callback owns delivery
to Telegram, a console, or another transport.

```yaml
agent:
  id: diagnostics
  trigger: diagnostics.message
  runtime:
    backend: redis
    prefix: agents:diagnostics
    url: ${AGENT_REDIS_URL}
  state_dir: state
  model: ${AGENT_MODEL}
  utility_model: ${AGENT_UTILITY_MODEL}
  prompt: prompt.md
  setup: diagnostics.tools:setup
  tools:
    - diagnostics.tools:QueryLogs
```

### Telegram group manager

`examples/telegram_group_manager.py` is the canonical conversational transport
demo. Telegram messages, local CLI input, ambient/Grafana observations,
announcements, and subagent updates enter one typed mailbox and agent-owned
event history. Ordinary group chatter is recorded but triaged away; useful
questions are answered after safe-point drains, so messages arriving during
the artificial work stages can join the same model call.

Telegram is only a producer and delivery adapter. `/announce TEXT` demonstrates
the own-message problem explicitly: the ambient event is recorded first, then
sent to Telegram with a delivery receipt, so the agent knows about a message
which Telegram's `getUpdates` will never echo back to the bot.

```bash
export TELEGRAM_BOT_TOKEN=...
export TELEGRAM_ALLOWED_CHAT_IDS=-123456789  # comma-separated; unset denies all
export TELEGRAM_GROUP_CHAT_ID=-123456789     # optional for a single allowed chat
export TELEGRAM_BOT_NAME=Alexander
export OPENAI_API_KEY=...
python3 -m examples.telegram_group_manager
```

The demo defaults to the coherent in-memory backend family. For a durable NAS
run, select Redis without changing application code:

```bash
export GROUP_MANAGER_RUNTIME_BACKEND=redis
export GROUP_MANAGER_REDIS_URL=redis://localhost:6379/0
export GROUP_MANAGER_RUNTIME_PREFIX=entourage:group-manager
python3 -m examples.telegram_group_manager
```

The CLI commands are `/ambient`, `/announce`, `/subagent`, and `/quit`. Disable
Telegram group privacy through BotFather when the bot must observe ordinary
group chatter rather than only commands and direct mentions.

The event history is persistent under `data/telegram-group-manager/`. With the
memory backend, pending mail remains process-local; Redis stores the mailbox
events, leases and deduplication keys. This demo uses the family's mailbox, with
history writes and Telegram delivery performed separately. It does not use an
execution graph or the new atomic session checkpoint API. Migrating the transport
demo to that API remains follow-up work.

---

## Architecture

- **`entourage/flow.py`** — the combinators: `Sequence`, `Parallel`, `Conditional`, and the
  policy-carrying `Node` leaf (retry/timeout controls).
- **`entourage/runtime/`** — the scheduler. One engine (`QueueRuntime`) uses three backend
  strategies: `GraphStore` (the persistent execution graph — in-memory, SQLite, and Redis),
  `ReadyQueue` (pointers to ready work — in-memory, Redis with fair-share claiming per
  session, and AWS SQS), and `Mailbox` (typed conversation events — in-memory and Redis).
  The Redis family puts the whole runtime state on one server:
  a durable, multi-worker deployment with ~3 ms/node orchestration overhead.
  The in-memory pair powers the local `Runtime` used by the examples; any durable
  store+queue combination gives fault-tolerant, resumable runs. The graph algebra (ready
  detection, fan-in joins, plan splicing) is shared code, tested identically across
  backends (`tests/`).
  A node's return is a *proposed transition*: the runtime stages any returned plan
  first, then commits result state, spliced plan, and rewiring as **one atomic store
  operation** (`commit_transition` — a SQLite transaction, a Redis MULTI). A crash
  mid-commit therefore re-runs the node on recovery instead of silently losing the plan
  it returned; fault-injection tests in `tests/test_transition_commit.py` pin this down.
  Computation proposes; the runtime commits.
  Coordination-aware nodes return a full `entourage.transition.Transition` — plain
  `state` and `(state, plan)` returns remain sugar for it. Its `acknowledge` and
  `publish` fields are **mailbox effects riding the same commit** (transactional
  outbox): recorded atomically with the completion, applied to the mailboxes right
  after, cleared once applied, and replayed idempotently at recovery — force-ack
  (the commit, not the lease, proves incorporation) plus deterministic publication
  `event_id`s make replay exactly-once-effective. Publication targets are opaque
  names mapped by an injectable `mailbox_resolver`. A delivery failure never fails
  the committed node; pending effects wait in the outbox index for the next replay
  (`tests/test_transition_effects.py`).
- **Retention** — terminal execution graphs are collected incrementally under a
  configurable TTL/count/batch policy on every graph backend. Acknowledged
  mailbox payloads and their idempotency tombstones have separate retention;
  typed conversation history rotates to an append-only archive. See
  [`docs/retention.md`](docs/retention.md).
- **`entourage/agent.py`** — high-level helpers that package the one-line Reason–Act pattern
  and compile down to the same `Sequence`/`Parallel` primitives the workers understand.

Long-running tools and human-in-the-loop steps go through the same dispatch/result-queue
path, so workers never block: the runtime parks the plan, frees the worker, and resumes
wherever the result lands — even days later.

See `ARCHITECTURE.md` for more.

---

## Status and limitations

Entourage is a programming-concept experiment, offered as an invitation to use the
primitive rather than as a drop-in dependency. The calculus is deliberately minimal —
three combinators — and there is not yet typed-plan support, a principled scheduling
policy, or a quantitative comparison against incumbent frameworks. The reference
implementation may lag the design.
