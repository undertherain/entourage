# Registered resumable agent

An ordinary [agent.py](agent.py), adjacent [manifest](agent.yaml), [prompt](prompt.md)
and mock [tools](tools.py). The runtime owns registration, restoration, leases and
commits. No graphs, model keys or external services are needed.

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

The mock result deliberately covers indoor and outdoor options so the amended
brief can still use it. Deciding to discard stale results or issue revised
requests belongs in application code. This demo accepts one correction identity
and one request per Research session; repeated delivery deduplicates them.

Definitions are checked against a durable contract in the session database:
entrypoint source, declared resources, configuration and state schema must match
on restart. Change the version when changing these. `development: true` explicitly
allows source/resource edits under the same version; it does not migrate state or
permit schema/config changes. Fingerprints do not package or pin imported
dependencies or the Python environment.

See [the runtime contract](../../../docs/resumable-executables.md) for failure,
batching, residency and implementation limits.
