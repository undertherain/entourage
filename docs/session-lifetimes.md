# Session lifetimes: keying, completion and retention

2026-09-22. Implemented in [`entourage.session_ingress`](../entourage/session_ingress.py),
`Context.spawn` in [`entourage.executables`](../entourage/executables.py) and the
`spawn`/`purge` operations of the [session backend](session-backends.md).

## A session has no intrinsic lifetime

A session is a durable unit of state, inbox, lease and status. Whether it lives
for one message, one task or forever is not a runtime property. It follows from
two application decisions the runtime does not make:

1. **Keying** — which session an event reaches. Decided at ingress for external
   events, or by a parent activation that spawns a child for delegated work.
2. **Completion** — when the handler proposes `complete=True`. Until then the
   session parks between activations and stays addressable.

The runtime owns only leases, residency (see [shards](runner-shards.md)) and
retention. The three familiar shapes are three keying/completion pairs:

| Shape | Key | Completes | Concurrency |
| --- | --- | --- | --- |
| Triage per message | `event`: `<alias>:<event_id>` | after one activation | every message in parallel |
| Task per request | spawned by a parent: `<parent>:<key>` | when the task is done | one activation per task |
| Conversation per chat | `conversation`: `<alias>:<conversation>` | rarely; closing the chat | serial per chat, parallel across chats |
| Concierge | `singleton`: fixed ID | never, only compaction and rebind | strictly serial |

**Concurrency is the number of sessions.** One session runs one activation at a
time, so a long-lived Concierge serializes everything addressed to it, and a
slow tool loop delays every later message in that session. Parallelism comes
from more sessions, not from residency or worker count. That is the reason to
keep group triage out of the Concierge session: give it event keying, or make it
a plain function inside the ingress adapter when its decision need not be durable.

Concierge is therefore not a special kind of session. It is the singleton
member of its shard plus the parent that spawns task sessions.

## Ingress keying

```python
from entourage.session_ingress import Member, SessionIngress

ingress = SessionIngress(store, [
    Member("triage", "triage:v1", "event"),
    Member("chat", "concierge:v1", "conversation", initial_state={"phase": "ready"}),
    Member("concierge", "concierge:v1", "singleton", session="concierge-main",
           initial_state={"phase": "ready"}),
])
delivery = ingress.deliver("chat", event, conversation="tg:42")
delivery.session_id, delivery.created, delivery.appended
```

`route` derives the ID without touching storage. `deliver` creates the session
on first use with a copy of `initial_state` and appends the event idempotently.
An existing session is never reset, so restart and concurrent adapters are safe:
one creator wins and every delivery appends. Creation and append are two backend
operations; a crash between them leaves a mail-less ready session whose first
activation sees an empty batch, which handlers must already accept.

Aliases are deployment names without `:`; the executable is the versioned
definition the shard registers. Appending to a complete session raises
`ValueError`: under event keying that is a replay carrying a new event ID, under
conversation or singleton keying the handler closed that key. The old graph-side
`IngressRouter` in `entourage.ingress` remains for graph consumers; this module
is the equivalent decision for the session core and imports no transport.

## Spawning task sessions

A parent activation stages a child with `context.spawn(definition, state, key=...)`
and receives the child ID `<parent>:<key>`. The child is created in the parent's
checkpoint transaction, before publications, so the same proposal can send it a
brief or request:

```python
child = context.spawn("research:v1", {"phase": "ready"}, key="research:1")
state["pending"] = context.request(child, {"brief": brief}, key="research:1")
return context.propose(state, incorporated=incorporated)
```

Keys are stable across retries. If the parent's attempt fails before commit,
nothing is created; the retry spawns the same child once. If the child already
exists, the backend raises `SessionAlreadyExists` and the whole proposal rolls
back, which makes a repeated logical spawn a visible error rather than a
duplicate task. The child's definition must be bound in the store; it may be
served by another dispatcher or runner in the same shard.

## Parent and child rule

Children are independent sessions. Parking, completing or purging the parent
cancels nothing and completes nothing. A reply addressed to a complete parent is
rejected at the child's commit, so the child's activation fails and retries after
lease expiry. Two consequences for authors:

- A parent that spawns and then completes must have collected its replies first,
  or pass a long-lived collector as the request destination.
- A child that must survive its parent's closure should send results to a
  singleton, not to the parent.

After `max_attempts` such rejections the child is parked as `failed` and the
runner reports it; see [deployment shards](runner-shards.md).

## Retention

Completed sessions keep their row and retained input IDs for duplicate
detection. That is bounded for a Concierge and unbounded for event keying.
`store.purge(completed_before=unix_time, limit=None)` deletes complete sessions
whose completion time is older than the cutoff and returns the count. It never
touches ready, active or waiting sessions, and never a session completed before
the completion-time column existed.

Purging ends duplicate detection for the removed sessions: the ID becomes unknown,
a delivery with the same key starts a fresh session, and a replayed event ID is
accepted again. The retention window must therefore outlast every transport's
redelivery window. Scheduling the purge is a deployment concern; the shard runner
is the natural owner. History retention for conversations remains an application
policy layered above, per [conversation mailboxes](conversation-mailboxes.md).

## Not covered here

Rebinding a long-lived session to a new definition version is described in
[session upgrades](session-upgrades.md); `list_sessions` provides enumeration.
Lease renewal and attempt limits are separate items in [NOW.md](../NOW.md). Cross-shard keying needs qualified routes and an outbox.
