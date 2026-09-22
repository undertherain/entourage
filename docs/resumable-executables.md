# Resumable executables — local Python contract

2026-09-22. Implemented in `entourage.executables`, following the consumer slice in
[TODO.md](../TODO.md#next-consumer-state-resumable-mailbox-agents-2026-09-22).
This narrows the earlier
[executable lifecycle proposal](executable-lifecycle.md) to a registered Python
dispatcher first. Graph integration and automatic process launching can follow.

The dispatcher now depends on [SessionBackend](session-backends.md), with
`LocalSessions` as the SQLite adapter. Backend-neutral types and the atomic
checkpoint contract are in `entourage.session_backend`.

## Foundation

`LocalSessions` already restores JSON state and pending mail under an expiring
lease, then atomically commits state, incorporated inputs, local publications and
a wait/deadline/completion condition. Executable IDs are opaque strings.

`examples/mailboxes/resumable.py` saves `next=finish`, exits, then a fresh process
looks up that function through `support.run_once`. This proves explicit
continuation restoration. Registration, dispatch and request/reply helpers are
still example scaffolding. In particular, that older `finish` assumes the matching tool
result is present: a user amendment alone would wake it and fail the lookup.
The new [registered example](../examples/mailboxes/registered/README.md) handles
that case through the public dispatcher and proposal API.

## The abstraction

A **resumable executable** is versioned code with a known re-entry function that
accepts restored state and delivered mail and returns a checkpoint proposal:

```text
resume(context, state, mail) -> proposal
```

The proposal contains updated JSON state, explicitly incorporated input IDs,
outgoing mail and the next wait/deadline/completion condition. The dispatcher
validates and commits it through the existing session store. Returning from the
function is not itself a checkpoint. Lease credentials and the authoritative
delivered-input set remain private to the dispatcher; handler inputs are copies.

Three identities keep code, durable work and attempts separate:

| Thing | Owns |
| --- | --- |
| Executable definition, e.g. `research:v1` | Code binding, entrypoint, state schema version, logical inbox contract |
| Session, e.g. `research:trip-42` | Definition binding, concrete inbox address, state, pending request identities |
| Activation | One leased attempt to resume that session from its last committed state |

A resident dispatcher can serve many sessions using the same definition. Every
activation restores committed state, including when the process is already warm.
Process residency is independent of the durable continuation.

## Manifest and mailbox binding

Local manifest, loaded with `Executable.from_manifest(path)`:

```yaml
definition: research:v1
protocol: entourage.activation/v1
entrypoint: agent.py:resume
state_schema: 1
inbox: session
resources: [prompt.md]
config:
  output: ui
```

The manifest declares that code consumes its session inbox. Session creation binds
the concrete address; it must not make every instance of `research:v1` compete
for one shared mailbox. The local implementation already addresses mail by
session ID, so this first slice needs no new subscription mechanism.

An external adapter resolves a conversation or other application identity to a
session and appends mail. The dispatcher finds ready sessions, resolves their
definition, restores their state and calls the registered entrypoint. These are
two distinct routing decisions: **mail to session**, then **state to behavior**.
Transport subscriptions and event-kind filters do not grant access authority.

Start with one inbox per session for user messages, correlated tool replies and
timers. Named shared ingress, multiple inboxes and distributed subscriptions can
be added when a consumer needs them. Local publication destinations must exist or be
spawned in the same proposal; ingress keying and child spawning are described in
[session lifetimes](session-lifetimes.md).

Python registration and YAML use the same definition. Entrypoints can be local
`file.py:function` paths relative to the manifest or importable
`package.module:function` names. File entrypoints do not modify `sys.path`;
application dependencies should be importable packages. The prompt and tools are
application concerns; the runtime only passes JSON `config` to the context.

Registration persists the schema, config, handler name and SHA-256 fingerprints
of the entrypoint file and declared resource files in `wake_definitions`, in the
same database as sessions. A mismatch rejects reuse of that definition version.
This checks local files, not a reproducible environment: imported dependencies,
interpreter versions and external assets are not automatically pinned. List
additional local dependencies as resources when their changes should be checked.
`development: true` explicitly omits source/resource fingerprints while still
binding schema/config. It does not migrate saved phase names or state. A new
definition version does not automatically rebind existing sessions; it declares
`upgrades: {old:v1: agent.py:migrate}` and the dispatcher migrates each old
session lazily when it next wakes. See [session upgrades](session-upgrades.md).

## The mini-router belongs inside the executable

Use one `resume` entrypoint by default. An ordinary Python phase table is an
optional authoring helper, not a required workflow language:

```python
# Ordinary application code; ingest and these handlers are application functions.
PHASES = {
    "ready": begin_research,
    "waiting_for_sources": continue_research,
}

def resume(context, state, mail):
    updated, incorporated = ingest(state, mail)
    return PHASES[updated["phase"]](context, updated, incorporated)
```

`ingest` is agent-owned logic: apply amendments, match replies against pending
request IDs and record any results needed later. The phase handler decides
whether enough information exists to advance or whether to park again. The
returned proposal carries those incorporated IDs; receipt alone is not
acknowledgment. Unknown input/phase behavior must be explicit rather than silently
dropping mail. Leaving mail pending keeps a session runnable, so it is not a
substitute for recording a handled amendment and waiting.

For example, in `waiting_for_sources`, a user correction updates the brief and
parks with the request still pending. A matching result lets the agent assess it
against the amended brief and either finish or issue revised work. A batch may
contain both. Whether an old result is still useful is application policy.

Saving a continuation means **where to reconsider work**, not a promise about
which event will arrive next. A simple agent can express all of this in one
function without a phase table. There are no graph edges to author or persist.

## Runtime API

```python
from pathlib import Path
from entourage.executables import Dispatcher, Executable
from entourage.sessions import LocalSessions

store = LocalSessions(Path("sessions.db"))
worker = Dispatcher(store, max_events=64, lease_seconds=30)
worker.register(Executable.from_manifest(Path("agent.yaml")))
worker.create("research:trip-42", "research:v1", {"phase": "ready"})
result = worker.run_once()
```

Alternatively register `Executable("research:v1", resume, state_schema=1)`.
`run_once()` returns `None` when no registered work is ready, otherwise a
`DispatchResult(session_id, committed, error)`. Inspect failures explicitly.
`run_until_idle(max_activations=100)` returns results up to that budget;
`run_forever(stop_event, poll_interval=0.1, on_result=None)` polls residently,
logging failures by default. Waiting holds no session lease. Unknown executable
versions are left for another worker; duplicate local registration is rejected.
`store.inspect(session_id)` observes committed state without consuming mail.

Handlers receive a fresh `Context`, a copied state dictionary and copied mail.
Context exposes `session_id`, `definition`, per-attempt `activation_id`, `config`,
`has_more` and, during a migration, `upgrading_from`. Its helpers stage effects; they do not send or commit immediately:

| Helper | Result |
| --- | --- |
| `context.send(destination, payload, key="answer:1", kind="message")` | Stable event ID; stages local publication |
| `context.request(destination, payload, key="sources:1")` | Stable request ID; stages request with session reply address |
| `context.reply(request, payload, key="result")` | Stages a correlated result with a stable publication ID |
| `context.spawn(definition, state, key="research:1")` | Stages child `<session_id>:<key>` bound to a registered definition; returns its ID |
| `context.propose(state, incorporated=ids, deadline=None, complete=False, rebind=None)` | Returns a `Proposal` including staged publications and children; `rebind` hands the session to another bound definition |
| `context.propose(state, incorporated=ids, deadline=None, complete=False)` | Returns a `Proposal` including the staged publications |

Keys identify logical operations and must be stable across retries and unique
within the sending session. Reusing a key for different work can deduplicate away
the new publication. Store outstanding request IDs in state; match both correlation
and the expected sender before accepting results. The helpers do not implement
an authorization system or runtime-managed exchange registry. Replies have
`kind: result`, `request_id`, `source` and `payload`; requests additionally carry
`reply_to`. Destinations must already exist, including output adapter mailboxes,
unless spawned by the same proposal: children are created before publications
are delivered. An existing child rejects the whole proposal, so a repeated
logical spawn surfaces as a failed activation rather than a duplicate task.

## Batches, failure and scope

The dispatcher defaults to 64 events per activation. `context.has_more` means
additional mail was already pending at claim time. An agent can incorporate this
batch into state and park, then decide whether to advance after the remaining mail
has been ingested. The registered example does this so a result in one batch
cannot trigger an answer before a queued correction in the next batch.

Mail can also arrive after claim. It remains pending; completion refuses to
discard it. A rejected completion rolls back state, acknowledgments and local
publications together, and is retried after lease expiry. A handler must explicitly
incorporate delivered inputs; leaving them pending makes the session ready again.
Claims rotate by durable last-claim order within each definition; a dispatcher
rotates between its definitions. This is basic local fairness, not worker-pool
admission, priorities or reserved capacity.

Handler errors, invalid proposals and expired-lease commits return a failed result
and leave the previous checkpoint and unincorporated mail intact. Infrastructure
errors can have an unknown commit outcome; `committed=False` means success was
not confirmed, and recovery follows durable state and leases. After an uncommitted
lease expires another attempt can run. There is no attempt limit, lease renewal or
dead-letter policy yet. A slow or stuck Python call cannot be killed by this
adapter; the lease fences its eventual commit but does not stop computation or
undo direct external effects. Event-count and dispatch-count budgets do not
bound payload bytes, memory or handler wall time. Use idempotency/reconciliation
for effects performed outside staged local mail.

This is a trusted in-process Python interface, not isolation from hostile code.
Warm memory is not authoritative state. No arbitrary stack is persisted, and
parking or completing does not cancel children or outstanding requests. Runtime
status (ready/active/waiting/complete) remains separate from application phase.
Subprocess management, packaged code environments, graph integration and remote
delivery remain follow-ups in [NOW.md](../NOW.md).
