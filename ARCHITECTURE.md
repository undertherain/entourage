# Architecture

Entourage runs LLM agents and workflows as **durable sessions**. The principles,
decided 2026-10-05 in [mailbox-first scheduling](docs/mailbox-first-scheduling.md):

- A **session** is explicit JSON state, its own mailbox and a lease. Nothing else:
  no execution graph, no persisted Python stack.
- A **definition** is versioned code with one entrypoint,
  `resume(context, state, mail) -> proposal`. Many sessions share one definition.
- An **activation** is one leased attempt to resume a session: restore state and a
  bounded mail batch, run handler code, commit the proposal (state, incorporated
  inputs, outgoing mail, children, next wait) atomically, release.
- **Readiness is durable data**: a session is runnable when it has unincorporated
  mail or a passed deadline. Creation is the first checkpoint, every wake runs the
  same entrypoint, completion is the last checkpoint. There is no wait node.
- **Continuation is application state** (a phase, a plan list, a pending-exchange
  table), so the handler can re-decide it at every wake and absorb steering.
- **Between sessions** there are three relations: fire-and-forget `send` to an
  identity-keyed address, `request`/`reply` to an owned child spawned in the
  parent's checkpoint, and `request`/`reply` to an existing service session. The
  parent's mailbox is the return address and stays open to unrelated mail.
- **Within a session** a step is plain Python, and a checkpoint between steps is
  `propose(deadline=NOW)`. One model call per activation is the default.
- **Concurrency is the number of sessions.** One session runs one activation at
  a time; lifetime follows from ingress keying (per event, per conversation,
  singleton) and completion, not from the runtime.
- **Deployment is a shard**: agent folders launched as worker processes or
  containers over one store, supervised by a runner that reads the store. The
  store is the protocol.

Modules: `sessions` (SQLite backend), `session_backend` (the contract),
`executables` (definitions, `Context`, `Dispatcher`), `session_ingress` (keying),
`exchanges` (pending request/reply table), `turn` (one model call with tools),
`runner` and `worker` (shards). `runtime/`, `flow`, `transition`, `mailbox`,
`ingress`, `monitors`, `conversation` and `agent` are the retired graph runtime,
kept importable for existing consumers.
