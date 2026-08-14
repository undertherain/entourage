# Runtime retention and garbage collection

Entourage bounds three independent storage layers. Collection is incremental
and never removes active execution sessions or unacknowledged mailbox events.

## Execution graphs

`RetentionPolicy` is enabled by default on `QueueRuntime`:

```python
RetentionPolicy(
    terminal_ttl_seconds=7 * 24 * 60 * 60,
    max_terminal_sessions=1000,
    batch_size=100,
    interval_seconds=60,
)
```

The runtime periodically selects completed or failed sessions which exceed the
age or count policy and asks the active `GraphStore` to delete the session,
executions, and edges atomically. In-memory, SQLite, and Redis stores implement
the same contract. Redis maintains a sorted terminal-session index, so routine
collection does not scan its namespace.

Set `retention_policy=None` to disable automatic collection, or call
`runtime.collect_garbage(force=True)` for an administrative sweep. Each call
deletes at most one configured batch.

Running sessions are structurally ineligible. Active-prefix compaction for a
future session that waits on a mailbox for weeks is a separate checkpointing
feature; deleting arbitrary completed nodes from an active DAG would destroy
state and lineage.

## Mailboxes

Acknowledged event payloads may be pruned in bounded batches after their durable
event-history handoff. Lightweight idempotency tombstones remain for a separate
retention window so Telegram or another producer cannot recreate a recently
collected turn by redelivering the same `event_id`.

The group-manager demo prunes acknowledged payloads immediately and retains
deduplication keys for seven days. Unacknowledged and leased payloads are never
eligible for payload collection.

## Conversation events

`EventHistory` retains a hot JSON window of 1,000 typed events by default.
Older events rotate into an append-only `.events.archive.jsonl` file. The group
manager sends only the latest 80 hot events to its models; model context bounds
and durable history bounds are deliberately independent.

Archive aging, topic-summary compaction, and a SQLite event-history backend are
future policies. The append-only archive prevents RAM and rewrite cost from
growing with the entire conversation, but deliberately continues consuming
disk until an operator-selected archival policy exists.
