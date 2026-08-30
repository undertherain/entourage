# Durable conversation mailboxes and bounded history

Status: in-memory and Redis mailbox strategies implemented; graph-integrated
runtime waiting sessions are not implemented. Recorded 2026-08-14 from the
Concierge and KIP diagnostics-agent use cases.

## Decision

Telegram or any other channel is not conversation storage. A continuous agent
owns a durable event mailbox and conversation log. Telegram, CLI clients,
Grafana alerts, cron jobs, tools, and subagents are peer producers of normalized
events; channel adapters are delivery edges.

Entourage must eventually support a durable session which can wait without
occupying a worker, wake when mailbox events arrive, and ingest them at explicit
safe points during a run. The current implementation instead creates one
execution session per incoming turn and uses `conversation_id` only to recover
logical history across those sessions.

This distinction is required because the Telegram Bot API does not deliver a
bot's own outgoing messages through `getUpdates`. A Grafana summary or another
client's bot message is invisible to Concierge if it is sent directly to
Telegram. Relevant outbound activity must be published to the agent mailbox
before, or alongside, channel delivery.

## Three independent histories

Do not collapse these into one retention mechanism:

1. **Conversation event log** — what the participants and external producers
   communicated. It carries durable normalized events and delivery receipts.
2. **Model context** — a bounded projection assembled from the current topic,
   recent summaries, durable facts, active work, and newly ingested events.
3. **Execution graph** — how a particular unit of work ran: nodes, attempts,
   intermediate state, failures, and scheduling edges.

Graph garbage collection must not erase conversation memory. Conversation
compaction must not decide whether a failed execution can be retried.

## Event envelope

A mailbox event should contain enough identity and lineage to be idempotent and
route results without treating every input as user speech:

```text
event_id
conversation_id
kind: user | ambient | subagent | tool_result | system
source
content or payload_ref
created_at
correlation_id
reply_target
ingested_at / acknowledgement
```

Large or sensitive results should enter by reference rather than being copied
through graph state and conversation history. Log text and other external
content is untrusted data, never instructions.

`kind` is semantically important. A Grafana summary is an ambient observation,
not a user message. A subagent progress update is neither a tool result nor an
assistant response. Prompt assembly decides how each kind is represented to the
model.

## Ingestion semantics

Events arriving while an agent works are appended durably immediately, but are
introduced into agent state only at declared safe points. They must not mutate
the context of an in-flight model or tool call.

Initial safe points:

- before a model invocation;
- after each tool result;
- after a subagent update or completion;
- before publishing a final answer;
- whenever long work voluntarily yields.

The intended loop is conceptually:

```text
receive or wake
  -> drain mailbox checkpoint
  -> reason
  -> schedule tool/subagent
  -> yield
  -> drain mailbox checkpoint
  -> continue, reply, or wait
```

Per-conversation serialization remains necessary, but it is not sufficient:
serial triggers queue whole turns behind one another, whereas mailbox
checkpoints allow an interjection or subagent update to join the active unit of
work at a controlled boundary.

## Runtime primitives to design

Names are provisional; behavior is the contract:

- A durable mailbox keyed by stable agent/session or conversation identity.
- Idempotent append and acknowledgement keyed by `event_id`.
- `DrainMailbox` (or equivalent) to claim available events at a safe point and
  record the checkpoint in persistent state.
- `WaitForMailbox` to park a session without holding a worker.
- Event append wakes a waiting session and places only a ready-work pointer on
  the queue.
- Recovery must not lose events between claim, state persistence, and
  acknowledgement. ~~The precise lease/transaction boundary needs design and
  fault-injection tests.~~ Settled and implemented for graph-node consumers:
  the graph-store commit of a node's `Transition` is the linearization
  point. Acknowledgements and outbox publications are recorded inside that
  commit, applied to the mailboxes afterwards, and replayed idempotently at
  recovery (force-ack ignores expired leases — the commit is the proof of
  incorporation; publications carry deterministic `event_id`s the mailboxes
  dedupe on). Fault-injection tests: `tests/test_transition_effects.py`.
  Loop-based consumers (`AgentWorker`) keep plain lease/ack semantics — the
  mailbox is a contract plane; the durable engine is one consumer of it.
- An interrupt or cancellation policy distinct from ordinary informational
  interjections.

This should compile into the same Control-by-Return execution machinery rather
than creating a second agent runtime.

## Producer and delivery rule

Relevant agent-visible activity follows this ordering:

```text
producer -> durable mailbox/event log -> agent processing
                                  \----> channel delivery + receipt
```

For example, Grafana should publish an ambient event and request Telegram
delivery rather than only calling Telegram. If an external system continues to
send directly through the Bot API, the agent cannot reconstruct those messages
from Telegram afterward; the producer must explicitly mirror them.

Delivery receipts belong in the event plane but normally do not enter model
context. Retries must be idempotent and must not manufacture duplicate
conversation turns.

## Retention and garbage collection

Retention is policy-driven and independent per layer.

### Conversation/event log

- Never remove unacknowledged mailbox events.
- Keep the current topic or active work verbatim.
- Compact concluded topics into summaries while retaining provenance pointers.
- Retain durable facts and decisions independently from raw chat turns.
- Apply explicit time/count policy to acknowledged raw events.

Implemented for the current demo: acknowledged in-memory mailbox payloads are
pruned after durable history handoff, seven-day idempotency tombstones prevent
immediate redelivery duplicates, and `EventHistory` rotates beyond its bounded
hot window into append-only JSONL.

### Model context

- Rebuild rather than append forever.
- Include a bounded live segment, selected summaries/facts, active task state,
  and newly drained events.
- Keep token budgeting separate from durable retention.

### Execution graph

- Never collect active, waiting, retryable, or externally dispatched work.
- Retain completed/terminal graphs for a configurable TTL or maximum count.
- Permit compaction to terminal metadata, lineage, outputs/receipts, and failure
  summaries after the debugging window.
- Remove graph records only after referenced results and mailbox checkpoints are
  durably owned elsewhere.
- GC must be incremental, namespace-scoped, observable, and safe to retry.

Implemented for terminal sessions: `RetentionPolicy` applies TTL, maximum-count,
batch, and interval bounds across in-memory, SQLite, and Redis graph stores. See
[`retention.md`](retention.md). Active-prefix compaction remains dependent on
the future waiting-session checkpoint design.

## Delivery sequence

1. ~~Specify the mailbox interface and event/checkpoint state transitions.~~
   `entourage.mailbox.Mailbox` now defines idempotent append and leased
   claim/ack/release semantics.
2. ~~Implement an in-memory backend and deterministic interjection tests.~~
   `InMemoryMailbox` is the reference backend; `examples/mailbox_cli.py`
   demonstrates typed events joining work at safe checkpoints.
   `examples/telegram_group_manager.py` exercises wait-any conversation
   claiming, persistent typed event history, group triage, multi-source
   interjections, and recorded Telegram announcement delivery in one process.
3. ~~Implement the Redis backend with wake-up, leases, crash recovery, and
   duplicate-event tests.~~ `RedisMailbox` implements the same contract and is
   selected together with Redis graph/queue strategies by `RuntimeBackendConfig`.
4. ~~Add waiting-session support to the runtime and expose mailbox
   checkpoints as plans/nodes.~~ `flow.WaitForMailbox` is a plan leaf that
   parks its execution durably (status `waiting`, no worker held), wakes on
   claimable mail or timeout, and acknowledges drained events inside the
   transition commit. Transport-neutral result ingress landed alongside it
   (`entourage.ingress`). See
   [`coordination-plane.md`](coordination-plane.md) for the decided
   semantics (one journal, ingress-time demux, three timeouts).
5. Migrate Concierge from turn-level sessions as the first consumer.
6. Exercise the same contract with the KIP diagnostics bot, including ambient
   Grafana summaries and colleague conversations. Telegram turns migrated
   2026-08-30: `deployment.MailboxAgentWorker` runs a per-conversation actor
   session (`WaitForMailbox → handle_turn → publish_replies → re-park`,
   rotated before the session node limit), created and revived lazily by
   `QueueRuntime.register_actor`/`ensure_actors` whenever a registered
   conversation has claimable mail. Ambient Grafana events still enter via
   the bot's own alert store, not the mailbox — that half remains open.
7. ~~Add terminal execution retention metadata and incremental graph GC.~~
   Terminal graphs are now collected incrementally; active-session prefix
   compaction remains a later checkpoint feature.

## Acceptance scenarios

- A user sends a second message while a tool is running; it is ingested after
  the tool result and before the next model call.
- A subagent progress event joins the parent at the next checkpoint without
  becoming user-authored text.
- Grafana publishes a summary which is delivered to Telegram and is also known
  to Concierge despite Telegram not echoing bot messages.
- A worker crashes after claiming an event; recovery ingests it once logically.
- A waiting session consumes no worker and wakes on a new mailbox event.
- GC removes eligible completed execution detail without losing conversation
  events, summaries, delivery receipts, or retryable work.
