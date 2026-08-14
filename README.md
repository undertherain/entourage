# Entourage

**Entourage is a small Python framework for building LLM agents and workflows as one
thing.** Steps are pure functions that *return* a declarative plan instead of calling each
other; a runtime executes the plan against a persistent graph, so runs are durable,
resumable, and replayable. The pattern it's built on is called **Control-by-Return**.

> Status: research prototype / reference implementation — a vehicle for the idea, not a
> production framework. Expect rough edges and a small, deliberately minimal API.

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
- `--debug` enables verbose output and prints the generated graph:

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

`entourage.conversation` provides a configurable loop for an agent whose
conversation outlives any one incoming-message execution:

- `ConversationPolicy` selects automatic topic-shift detection and a manual
  reset command such as `/new`.
- `ContinuousConversation` owns the live segment, automatic/manual compaction,
  and prompt rebuilding around durable `ChatHistory`.
- `ContinuousAgent` supplies the main model/tool loop while the application
  supplies its tools and system-prompt builder.
- `TopicMemory` supplies the litellm-based detector, summarizer, and file
  archive; applications select its utility model and retention count.

This is logical conversation continuity over turn-level execution sessions.
A durable session that waits on a mailbox and resumes for later messages is a
separate runtime primitive still to be designed. The agreed event model,
safe-point ingestion semantics, and independent conversation/context/graph
retention policies are recorded in
[`docs/conversation-mailboxes.md`](docs/conversation-mailboxes.md).

For multi-agent deployments, `RedisRuntimeConfig` derives graph and ready-queue
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
  redis_prefix: agents:diagnostics
  redis_url: ${AGENT_REDIS_URL}
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

The CLI commands are `/ambient`, `/announce`, `/subagent`, and `/quit`. Disable
Telegram group privacy through BotFather when the bot must observe ordinary
group chatter rather than only commands and direct mentions.

The event history is persistent under `data/telegram-group-manager/`; the
mailbox itself is currently in memory. A process crash can therefore lose
events which have arrived but not yet reached history. Redis mailbox persistence
and graph-integrated waiting sessions remain the next runtime layer.

---

## Architecture

- **`entourage/flow.py`** — the combinators: `Sequence`, `Parallel`, `Conditional`, and the
  policy-carrying `Node` leaf (retry/timeout controls).
- **`entourage/runtime/`** — the scheduler. One engine (`QueueRuntime`) behind two backend
  seams: `GraphStore` (the persistent execution graph — in-memory, SQLite, and Redis) and
  `ReadyQueue` (pointers to ready work — in-memory, Redis with fair-share claiming per
  session, and AWS SQS). The Redis pair puts the whole runtime state on one server:
  a durable, multi-worker deployment with ~3 ms/node orchestration overhead.
  The in-memory pair powers the local `Runtime` used by the examples; any durable
  store+queue combination gives fault-tolerant, resumable runs. The graph algebra (ready
  detection, fan-in joins, plan splicing) is shared code, tested identically across
  backends (`tests/`).
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
