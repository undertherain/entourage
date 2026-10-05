# Entourage

**Entourage is a small Python framework for building LLM agents and workflows as one
thing: durable sessions.** A session is explicit state, its own mailbox and a lease. Your
code is one function that wakes with the saved state and new mail, decides what to do,
and proposes the next checkpoint. The runtime commits it atomically and releases the
worker. Runs survive restarts, take corrections while they wait, and can be read back
from the store at any time.

> Status: research prototype / reference implementation — a vehicle for the idea, not a
> production framework. Expect rough edges and a small, deliberately minimal API.

Current build state: [NOW.md](NOW.md). The decision that shaped the current design:
[mailbox-first scheduling](docs/mailbox-first-scheduling.md).

---

## The idea

LLM systems usually sit at one of two ends of a spectrum:

- **Workflows** — hand-wired graphs of steps (à la LangGraph, Airflow, Temporal).
  Deterministic, durable, each step easy to test and cheap to route to the right model.
  The cost is brittleness: a workflow can only do what it was wired to do.
- **Agents** — an LLM in a Reason–Act loop (à la LangChain, CrewAI, AutoGen). They handle
  novel inputs because the model picks the next action at runtime, but the orchestration
  lives inside one model's context window and the whole run sits in one process: a crash
  mid-tool loses it.

The only thing that really differs between the two ends is **where the decision about the
next step lives**: in the code you wrote, or inside a model at runtime. Entourage makes
that decision a value in session state, re-decided at every wake. A workflow stores a
phase name; an agent stores a conversation and asks the model. Both are the same session,
the same entrypoint and the same checkpoint.

### How it works

A **definition** is versioned code with one entrypoint:

```python
def resume(context, state, mail):   # -> context.propose(...)
```

A **session** binds a definition to durable JSON state and an inbox. An **activation**
is one leased attempt to resume it: the dispatcher restores state and a bounded batch of
mail, calls `resume`, and commits the returned proposal (new state, the mail it
incorporated, outgoing mail, spawned children, the next deadline or completion) in one
transaction. Returning from the function is not a checkpoint; the proposal is.

```python
from entourage.executables import Dispatcher, Executable
from entourage.sessions import LocalSessions

def resume(context, state, mail):
    for event in mail:
        state["seen"] = state.get("seen", 0) + 1
    return context.propose(state, incorporated=[e["event_id"] for e in mail])

store = LocalSessions("sessions.db")
worker = Dispatcher(store).register(Executable("counter:v1", resume))
worker.create("counter", "counter:v1", {})
store.append("counter", {"event_id": "m1", "kind": "user", "payload": {"text": "hi"}})
worker.run_until_idle()
```

A session is runnable when it has unincorporated mail or a passed deadline. Creation is
the first checkpoint, every wake runs the same entrypoint, completion is the last. There
is no "wait" step: every proposal that is not complete parks the session, holding no
worker.

Two things follow directly:

- **An agent is one model call per activation.** `entourage.turn.ChatAgent` keeps the
  conversation in state, runs tool calls inline, checkpoints the tool results and lets
  the next activation call the model again. A user message that arrives while tools run
  is seen by the model on the next wake, which is what makes steering possible.
- **A workflow is a phase table.** `state["phase"]` names where to reconsider; the
  handler reads it, does the step, writes the next phase. A checkpoint between two steps
  is `propose(state, deadline=NOW)`.

Between sessions there are three relations, and they are the whole coordination model:

| Relation | Shape | Example |
| --- | --- | --- |
| `send` to an identity-keyed address | fire-and-forget | triage hands a message to the chat's session |
| `request`/`reply` to an owned child | a pipe: the parent's mailbox is the return address | a subagent spawned in the parent's checkpoint |
| `request`/`reply` to an existing service | a call | a tool worker session |

`entourage.exchanges` keeps the pending table in state and splits delivered mail into
the replies a session waited for and everything else, so a parent can run an `all` or
`any` join and still take a correction while it waits.

### A worked example

A Telegram community-manager bot. One session per chat holds the typed history. A turn
is two checkpointed phases: triage on a cheap model, commit, then the answer on a strong
one, so messages arriving during triage join the answer's context. Replies are mail to a
`telegram-outbox` session whose handler calls the Bot API. Inserting an approval step
before delivery is address rerouting, not a code change in the chat. The example carries
a second triage shape as well: a per-event session that labels each message and forwards
it, parallel and stateless. See `examples/telegram_group_manager.py`.

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

A chat agent whose conversation is session state, with memory and search tools:

```bash
python3 examples/cli.py
```

- Interact with the agent in natural language. Kill the process mid-turn and start it
  again: the turn finishes from its last checkpoint.
- `/new` starts a fresh session (keeps long-term memory).
- `--debug` shows tool calls and runtime logging.

One question, no state kept: `python3 -m entourage "what's the weather in Tokyo?"`.

The [examples index](examples/README.md) lists the rest: a coding agent, the Telegram
group manager, children and supervision, waiting and late replies, retry policy, and a
shard launched by the runner. The standalone ones need no model key. Start with the
**[registered agent](examples/mailboxes/registered/README.md)** for the full authoring
contract: adjacent manifest, prompt and tools, a correction while a tool is pending, and
resumption in a fresh process.

---

## Sessions

- **Contract.** [Resumable executables](docs/resumable-executables.md): definitions,
  manifests, `Context` helpers, batches, failure. The dispatcher depends on a
  [session backend interface](docs/session-backends.md); SQLite is the implementation,
  with a conformance suite for future adapters.
- **Lifetime.** Keying plus completion, not a runtime property: per event, per
  conversation or singleton at ingress; parent-spawned children; retention by purge.
  [Session lifetimes](docs/session-lifetimes.md).
- **Upgrades.** A long-lived session moves to a new definition version lazily at its
  next wake through declared migrations. [Session upgrades](docs/session-upgrades.md).
- **Deployment.** A shard: agent folders run as worker processes or podman containers
  over one store, supervised by a runner that reads the store; eager or on-demand start,
  idle grace, reserved capacity, attempt limits, failure notices as mail.
  [Deployment shards](docs/runner-shards.md), [demo shard](examples/shard/shard.yaml).
- **Cost model.** A parked session holds no memory. Every hop between sessions is a
  checkpoint and a claim; steps inside a session do not touch a queue. Keep session
  state small and reference bulky data.

## Capabilities

`entourage.capabilities` composes agent behaviour by registration rather than
inheritance: contributive capabilities add prompt sections and tools and cannot
invalidate each other; at most one `ConversationLifecycle` owns history and decides what
the model sees for a turn. `builtin_capabilities` ships `Facts`, `RecentSummaries` and
`TopicShiftLifecycle`. These are application-level units with no runtime dependency; the
`ConfiguredAgent` and `ContinuousAgent` drivers that used them belong to the retired
graph runtime below.

## Telegram group manager

```bash
export TELEGRAM_BOT_TOKEN=...
export TELEGRAM_ALLOWED_CHAT_IDS=-123456789  # comma-separated; unset denies all
export TELEGRAM_GROUP_CHAT_ID=-123456789     # optional for a single allowed chat
export TELEGRAM_BOT_NAME=Alexander
export OPENAI_API_KEY=...
python3 -m examples.telegram_group_manager
```

Telegram messages, local CLI input, ambient observations, announcements and subagent
updates all enter the chat's session; ordinary chatter is recorded as context and
triaged away, questions are answered. `/announce TEXT` records the ambient event first
and then delivers it, so the agent knows about a message that `getUpdates` will never
echo back. `GROUP_MANAGER_TRIAGE=session` switches to per-event triage sessions. State
lives under `data/telegram-group-manager/sessions.db`; inspect it with
`LocalSessions.inspect`. Disable group privacy through BotFather when the bot must see
ordinary chatter.

---

## Architecture

The principles and the module map are in [ARCHITECTURE.md](ARCHITECTURE.md). In short:
`sessions` and `session_backend` hold the store contract; `executables` the definitions,
`Context` and `Dispatcher`; `session_ingress` the keying; `exchanges` the pending
request/reply table; `turn` one model call with tools; `runner` and `worker` the shard.
Persisted state is never a Python stack: restart happens at a known entrypoint.

### Retired graph runtime

Entourage began as Control-by-Return: nodes returned `Sequence`/`Parallel` plans that a
runtime spliced into a persisted execution graph. That runtime (`entourage.runtime`,
`flow`, `transition`, `mailbox`, `ingress`, `monitors`, `conversation`, `agent`) is
retired as of 2026-10-05 and replaced by the session core; the reasoning is in
[mailbox-first scheduling](docs/mailbox-first-scheduling.md). The modules stay
importable with a `DeprecationWarning` while existing consumers migrate, and their tests
still run. Graph scheduling turned out to be a special case of mailboxes: a successor
step is a single-use address that shares the session's state and lifetime.

---

## Status and limitations

Entourage is a programming-concept experiment, offered as an invitation to use the
primitive rather than as a drop-in dependency. The session core is SQLite-only and single
host; a Redis backend and distributed delivery are future work. There is no lease renewal
(the lease is the hard step limit), no replay of past checkpoints beyond the current
state, and no dead-letter policy beyond the `failed` status and the runner's notice. The
reference implementation may lag the design.
