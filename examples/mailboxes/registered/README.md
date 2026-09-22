# Registered resumable agent

An ordinary [agent.py](agent.py), adjacent [manifest](agent.yaml), [prompt](prompt.md)
and mock [tools](tools.py). The runtime owns registration, restoration, leases and
commits. No graphs, model keys or external services are needed.
`Dispatcher` accepts the [SessionBackend interface](../../../docs/session-backends.md);
this CLI chooses `LocalSessions`, its SQLite implementation.

Run from the repository root with a fresh database path. Each command starts a
new process:

```bash
python -m examples.mailboxes.registered.run /tmp/registered-agent.db start
python -m examples.mailboxes.registered.run /tmp/registered-agent.db correct "Quiet indoor activities in Kyoto"
python -m examples.mailboxes.registered.run /tmp/registered-agent.db show
python -m examples.mailboxes.registered.run /tmp/registered-agent.db tool
python -m examples.mailboxes.registered.run /tmp/registered-agent.db tick
python -m examples.mailboxes.registered.run /tmp/registered-agent.db show
```

After `start`, Research has dispatched a correlated request and parked. Events
has completed independently using the same executable definition. `correct`
restores Research, records the amendment and parks with the request still pending;
there is no premature final answer. `tool` publishes the result. `tick` restores
the amended state and commits the answer to the durable `ui` mailbox.

`serve` runs both registered definitions in one resident dispatcher until Ctrl-C.
Use it after `start` to process the request and result automatically. It uses the
same activation path as the fresh-process commands. The CLI/UI process itself
does not need to keep an activation open. `show` observes committed state without
claiming either agent's inbox.
It does not load executable source or validate definition fingerprints, so saved
state remains inspectable after code changes. `tool` loads only the tool definition.

The mock result deliberately covers indoor and outdoor options so the amended
brief can still use it. Deciding to discard stale results or issue revised
requests belongs in application code. This demo accepts one correction identity
and one request per Research session; repeated delivery deduplicates them.

Both agent sessions deliberately use `complete=True` after their one answer.
`correct` is an amendment while Research is pending, not a follow-up after it has
finished. A conversation that accepts later questions must park after answering
and assign new publication/request keys per turn. `serve` keeps the dispatcher
running; it does not reopen completed sessions or implement per-agent processes,
ten-minute idle grace, or automatic subprocess launch. Those are the next
[shard-runner proof](../../../docs/runner-shards.md).

Definitions are checked against a durable contract in the session database:
entrypoint source, declared resources, configuration and state schema must match
on restart. Change the version when changing these. `development: true` explicitly
allows source/resource edits under the same version; it does not migrate state or
permit schema/config changes. Fingerprints do not package or pin imported
dependencies or the Python environment.

See [the runtime contract](../../../docs/resumable-executables.md) for failure,
batching, residency and implementation limits.
