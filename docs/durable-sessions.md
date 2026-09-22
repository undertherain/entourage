# Durable session wakeups — first local slice

Current next steps: [NOW.md](../NOW.md) and
[execution runtime handoff](execution-runtime-handoff.md).

Experimental implementation, 2026-09-08. Follows IOA's
`docs/architecture/memos/durable-sessions-and-project-home.md` and revised ADR
0018: Entourage owns execution, Astral supplies a future distributed delivery
binding, and graph authoring is optional. Detailed authoring APIs remain open.

Added 2026-09-22: [registered resumable executables](resumable-executables.md)
provide a local Python authoring API and resident dispatcher over this same store.

`entourage.sessions.LocalSessions` supplies a SQLite-backed local binding with
no graph or model dependency. A session binds explicit JSON state to an opaque
executable version. An activation receives restored state and ordered pending
mail, holds an expiring write lease, and proposes a checkpoint. No Python stack
or coroutine is persisted.

The database supplies readiness directly. New sessions run once; parked sessions
wake on pending mail or an absolute Unix deadline. A crashed activation becomes
eligible when its lease expires. The scheduler caller polls `claim()`; no
notification, resident conversation object, or running session process is needed
to remember work. Deadlines require a polling scheduler to actually run them.

`claim(max_events=N)` bounds delivered event count and exposes `has_more` when
additional inputs remain queued. Omit the argument for the original unbounded
direct-call behavior. Claims rotate by durable last-claim order. The registered
dispatcher uses batches of 64 by default and rotates between its definitions.

`commit()` atomically persists state, marks the explicitly incorporated input IDs,
publishes to other local sessions, registers the next deadline and releases the
lease. Publication targets must already exist. Since all local mail is in the
same database, it participates directly in that transaction. Remote publication
will need a transactional outbox; calling Astral inside this transaction would
not provide that guarantee.

Mail arriving during an activation remains pending after it parks, so the next
claim sees it. Claiming and committing serialize against appends. Expired or
replaced lease tokens cannot commit; retries see the last committed state and
unincorporated inputs. Publication IDs are caller-supplied and deduplicated per
destination. A due deadline produces a stable `kind: system`, `source: timer`
event across activation retries; `timer:` IDs are reserved for the runtime.
Completion refuses to discard pending mail. Parking does not cancel children.

Try the example with each command in a fresh Python process:

```bash
python examples/mailboxes/wake_session.py /tmp/entourage-wake-demo.db start
python examples/mailboxes/wake_session.py /tmp/entourage-wake-demo.db tick
python examples/mailboxes/wake_session.py /tmp/entourage-wake-demo.db send "Two people"
python examples/mailboxes/wake_session.py /tmp/entourage-wake-demo.db tick
```

The first command saves a clarification phase and parks. The first tick has no
work. Appending the answer wakes that same identity, and the final tick restores
the phase and completes. Use a fresh database path for another run.

For tool request/reply and a tool that asks the user for clarification, see the
[mailbox tool examples](mailbox-tool-examples.md). These exercise correlated local
mail across fresh processes with an example-only handler dispatcher, using this
same core. They do not add executable registration or atomic child creation.
For registration, adjacent manifests and correction during a pending tool call,
use the [registered agent](../examples/mailboxes/registered/README.md).

Validation: `python3 -m pytest -q tests/test_sessions.py` covers fresh subprocess
resume, abrupt exit before commit, expired leases, concurrent claims, persisted
deadlines, mail during parking, input deduplication, rollback of local publication,
and isolation between two sessions sharing an executable.

Next: durable launch descriptors and a subprocess adapter, then the complete
two-trip parent/child clarification walkthrough from IOA. This slice does not
yet implement child creation, runtime-managed exchange tracking, executable
deployment, resource pools, lease renewal, retention, graph integration or Astral
delivery. Local fairness is basic rotation, without priorities or admission.
Tables use a `wake_` prefix and can occupy an existing SQLite file; no second
database is inherently required. Reusing the graph transaction/execution adapter
still needs an explicit integration step. Opening an older database adds the
claim-order column without replacing sessions; definition contracts share the
same file in `wake_definitions`.
