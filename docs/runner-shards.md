# Deployment shards: runner, workers and the store as protocol

2026-09-23. Implemented in [`entourage.runner`](../entourage/runner.py) and
[`entourage.worker`](../entourage/worker.py) with a process launcher and a
podman launcher. The 2026-09-22 proposal assumed the runner delivered steps to
workers over a protocol; that was dropped. The store is the protocol.

## The picture

A **shard** is one deployment's group of agents: a store, a set of agent folders,
and one runner. Second Brain is one shard. Nothing is global.

A **worker** is a process running one agent folder's code. It opens the store,
claims ready sessions of its definitions, runs a step, commits, and loops. It is
the existing dispatcher, started as `python -m entourage.worker`, either as a
plain subprocess or as a container of the generic runtime image with the agent
folder mounted. A worker decides its own exit: on SIGTERM, or after an idle
period without a claim.

The **runner** never talks to workers. It reads the store and manages processes:

| Store shows | Runner does |
| --- | --- |
| Eager member has fewer workers than reserved | Start one, with backoff after quick exits |
| Ready sessions for a member exceed its idle workers | Start one, within `parallel` and the shared pool |
| Active session whose lease expired and whose worker is still alive | Kill that worker |
| Session in status `failed` | Send one notice to the `notify` session |
| Worker vanished | Nothing; its session retries after its lease expires |

The worker's identity is recorded on the lease it holds, so the runner can map
an expired lease to a container to kill. That is the only thing the runner needs
that a worker writes.

## Manifest

```yaml
shard: second-brain
store: ./state/sessions.db
pool: 4                       # shared slots for on-demand workers beyond reserved
notify: concierge-main        # optional: session that receives failure notices
launcher: {kind: podman, image: localhost/entourage-runtime, env: [OPENAI_API_KEY]}

executables:
  concierge:
    folder: ./concierge         # contains agent.yaml
    runner: {start: eager, reserved: 1}
    worker: {idle_exit: never}
  events:
    folder: ./events
    runner: {start: on_demand, parallel: 2}
    worker: {idle_exit: 600, lease: 300, max_attempts: 3}
```

`runner` settings are decided by the runner, because only it sees the shard:
`start` (eager or on demand), `reserved` (workers this member always gets,
outside the pool; defaults to one for eager members) and `parallel` (most
workers at once). `worker` settings are passed to the worker as arguments,
because only it knows whether it is idle: `idle_exit` (seconds, or `never`),
`lease` (lock length of one step, which is also its hard limit) and
`max_attempts`. None of these are part of the definition contract, so tuning
them never changes a code version.

Aliases are deployment names; the versioned definition comes from the folder's
`agent.yaml`. Sessions are created by ingress keying or bootstrap, not by the
runner; an eager worker for a member with no sessions simply idles.

## Capacity

Reserved workers never come out of the pool, so an eager Concierge is never
starved by research bursts. On-demand workers beyond a member's reserved count
draw from `pool`. When the pool is exhausted the ready session waits in the
store; nothing is lost, it is late. Parallelism is the number of workers, and a
worker serves any ready session of its definitions. Pinning a worker to one
session exists (`--session`) but is not used by the runner; it only pays off
when a process holds per-session memory worth keeping.

## Stuck, crashed and poisoned

The lease is a watchdog, not a heartbeat. Normally the worker reports: a commit
is "finished", and a failed step is released at once with its error and a
backoff that doubles per attempt up to the lease length. Only a worker that
cannot report, because it died or hung, lets the lease expire.

- **Crashed worker.** The lease expires; the session is ready again; the runner
  starts a worker if needed. Frozen for at most one lease length.
- **Hung worker.** Same, plus the runner kills it to free the slot. Its commit
  was already fenced by the expired lease. In-process Python cannot be
  interrupted, which is why the worker is a separate process.
- **Poisoned session.** The store counts attempts per step and resets on commit.
  A ready session that already used `max_attempts` is parked as `failed` by the
  next claim, durably, and no worker is started for it. Mail is still accepted
  and kept. `store.retry(session_id)` returns it to service.

The step tells the agent where it stands: `context.attempt` and
`context.last_error`. On the third try an agent can use a smaller model, skip the
tool that hung, or tell the user it is stuck and park, which beats being killed
three times.

Failure notices are mail: `kind: system, source: runner` with the failed session,
its definition and attempt count, deduplicated per failure by event ID. An ops
agent or Concierge reads them like any other input.

## Podman

`Containerfile` builds `localhost/entourage-runtime`: Python, Entourage and the
common dependencies, entrypoint `python -m entourage.worker`. The podman
launcher runs each worker as

```
podman run -d --rm --name second-brain-events-7 \
  --label entourage.shard=second-brain --label entourage.agent=events \
  -v ./state:/state -v ./events:/agent:ro -e OPENAI_API_KEY \
  localhost/entourage-runtime --store /state/sessions.db --agent /agent \
  --worker second-brain-events-7 --lease 300 --max-attempts 3 --idle-exit 600
```

Workers see the store directory and their own folder, nothing else, and need no
network to each other: all traffic between agents is mail through the store.
The runner lists, stops and kills by label, and on start removes every container
carrying its shard label left by a previous runner. Stop is `podman stop -t 10`.
An agent that needs dependencies the runtime lacks builds its own image `FROM`
the runtime image; the manifest's `image` then names it.

The runner itself is a plain process under systemd, see
`deploy/entourage-runner.service`. It stops its workers on shutdown; a worker
that was mid-step has its session retried after the lease.

## Trust

A worker opens the store. Agent code that misbehaves can commit to any session
in the shard. That is the same trust the in-process dispatcher has and is
acceptable for a deployment's own agents on its own host. A stdin/stdout step
protocol, keeping the store away from workers, returns only when a worker cannot
share the store: Firecracker without a shared filesystem, or another machine.

## Demo

`examples/shard/shard.yaml` runs an echo agent with the process launcher:

```
python -m entourage.runner examples/shard/shard.yaml &
python examples/shard/send.py hello
python examples/shard/send.py --show
```

The runner starts a worker on the first message, the worker parks the session
and exits after ten idle seconds, and the next message starts a fresh one.

## Not built

Runner ownership lease (two runners on one store would both start workers),
lease renewal, per-container memory and CPU limits from the manifest,
workspaces as named volumes, external service bindings as manifest entries,
cross-shard mail, and the runner in a container.
