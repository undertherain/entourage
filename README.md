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

More examples live in `examples/` (`telegram_*.py`, `coding_agent.py`).

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
separate runtime primitive still to be designed.

For multi-agent deployments, `RedisRuntimeConfig` derives graph and ready-queue
namespaces from an application-selected prefix. Agents can share one Redis
deployment while keeping scheduler namespaces isolated when their workers
register different node sets.

### Minimal Telegram bot

`examples/telegram_simple.py` is a small continuous-conversation demo. Each
Telegram update pushes a `telegram_message` trigger onto Entourage's queue;
the listener never calls the model directly. The workflow stores incoming and
successfully delivered outgoing turns under `data/telegram-demo/history/`, so
Telegram is transport rather than conversation storage.

The chat ID is passed as the trigger's `serial_key`. If another message arrives
while that chat's session is still running, it remains queued and starts only
after the current session finishes; different chats can progress independently.

```bash
export TELEGRAM_BOT_TOKEN=...                # token from BotFather
export TELEGRAM_ALLOWED_CHAT_IDS=123456789   # comma-separated; unset denies all
export OPENAI_API_KEY=...
python3 -m examples.telegram_simple
```

Send `/new` to archive the current live history and start with empty context.
The demo uses an in-memory trigger queue for a one-process quick start and a
persistent SQLite execution graph. Swap in the Redis queue/store pair when the
listener and workers need to be separate processes or queued triggers must
survive a process restart.

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
