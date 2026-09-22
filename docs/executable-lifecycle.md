# Executable packaging and idle residency

Design review, 2026-09-09. Optional exit on mailbox wait is the user-directed
requirement. The schema and adapter order below are proposals, not implemented
APIs. See [NOW.md](../NOW.md) for the implementation baseline.

2026-09-22 update: the [local Python contract](resumable-executables.md) now
implements versioned registration, adjacent manifests and a resident dispatcher.
The process-launch and grace-policy schema below remains a proposal.

Further user refinement: [deployment shards](runner-shards.md) scope a runner to
an independently operated agent group and place residency in its deployment
configuration. Concierge can start eagerly and remain resident; Events can launch
on demand and remain warm for ten minutes. External service bindings cover an
always-on NAS OCR endpoint. This replaces the assumption of a required global
runner and supplies the next automatic-launch proof.

## What exists

- The graph runtime executes registered Python callables.
- `AgentManifest` describes configured Python conversational agents: prompts,
  models, imported tools/setup, state paths and storage backend. Relative paths
  resolve beside its YAML file. It is not a general executable launch manifest.
- `LocalSessions` binds durable sessions to executable version strings.
  `entourage.executables.Dispatcher` resolves registered versions and commits
  handler proposals. There is no registered subprocess launcher, container
  launcher or Firecracker adapter yet.

## Separate packaging, execution and residency

A folder with a manifest is a convenient authoring package. An execution backend
decides how to run that package. An idle policy decides whether its process stays
alive between activations. These should be independent choices.

Proposed support order:

1. Preserve Python callables for existing graphs and local embedding.
2. Add a folder plus versioned manifest identifying argv, cwd, protocol version,
   dependency environment, and resource requirements. Launch as a local process.
   Python, Node.js, compiled binaries or a wrapper can implement the same protocol.
   Arbitrary interactive CLIs need an adapter; argv alone does not confer durable
   state restoration or mailbox support.
3. Add image-based execution when deployment needs it. A container is a candidate;
   local Firecracker is another backend, not a competing manifest format. Do not
   require a container implementation before exploring Firecracker.

Registration must bind a definition version to resolved code/environment identity;
a mutable folder path alone cannot supply reproducible restart. Development can
explicitly opt into mutable code. Session state and writable coding workspaces
have separate storage lifetimes from code artifacts.

Firecracker requires Linux KVM access, a guest kernel and a root filesystem image
([official getting-started guide](https://github.com/firecracker-microvm/firecracker/blob/main/docs/getting-started.md)).
Our adapter would additionally need a guest runner speaking the activation
protocol, host/guest communication, workspace persistence and lifecycle control.
Initially restore explicit session state on launch. VM snapshotting can be an
optimization later; it does not replace the authoritative session checkpoint.

Illustrative manifest fragment, not accepted by today's `AgentManifest`:

```yaml
definition: travel:v1
protocol: entourage.activation/v1
launch:
  backend: process
  argv: [python3, agent.py]
  cwd: .
idle:
  mode: grace
  seconds: 10
```

## Waiting does not require exiting

Waiting commits explicit state, incorporated mail, outgoing effects and wake
conditions. The activation's write lease ends at that commit. Process residency
is a separate runtime decision:

| Policy | After a committed wait | Typical use |
| --- | --- | --- |
| release | Exit/release the process immediately | Many intermittently active user sessions |
| resident | Keep the process available for subsequent activations | Local interactive coding agent |
| grace, 10 seconds | Keep it available; release after 10 idle seconds | Bursty conversations and fast tool replies |

The grace interval starts after the waiting checkpoint is accepted. If more mail
is ready, the scheduler claims a new activation and may reuse the process. After
that activation commits another wait, the idle interval starts again. This is a
resource timer: its expiry does not insert a business timer event into the inbox.

Defaults belong in the executable/deployment configuration; a session can request
an override within runtime admission limits. Resident placement requires a
capacity decision and does not guarantee survival of crashes. Start with one
session per process; pooling or multiplexing many sessions is separate future work.
A resident process still consumes memory, but it holds no active session write
lease while idle and does not reserve an execution slot merely by waiting.

## Protocol consequences

Use repeatable activation/proposal/commit-acknowledgment messages rather than
assuming one stdin document and EOF is the only lifecycle. A process may serve
one activation and exit, or serve successive activations. The runtime retains
lease credentials and the authoritative delivered-input set. A process receives
activation identity, state and mail, then proposes its checkpoint and effects.
Only a successful runtime commit completes an activation; process exit is not
evidence of a commit. Logs need a separate channel from protocol messages.

Every resumed activation obtains a fresh lease, even on the same process. Warm
memory is a cache; restored durable state is authoritative. Idle process affinity
does not grant write authority. If another worker claims the session, an old warm
process cannot act without a new runtime activation.

Mail arriving during grace expiry or process shutdown remains durably ready.
The scheduler must recheck readiness and launch/reuse a worker as needed; no
delivery may depend solely on an in-memory listener. Grace expiry must retire
only an idle process, never kill a newly assigned activation. Assignment and
retirement need a coordinated worker lifecycle independent of the session lease.

A local CLI can keep its terminal composer alive and append typed messages to
the mailbox. It may also keep the agent process resident. Keeping the UI alive
does not require keeping an agent activation open while the user types. A receive
loop SDK can hide the repeated claims/checkpoints, but cold restart still enters
at a known point with explicit state, not a persisted Python stack.

## Next proof

Implement a local registered process adapter with the repeatable protocol. Run
the same mailbox tool example under release, resident and 10-second grace modes,
and run one original graph node through the same adapter. Verify process reuse,
cold restart, commit acknowledgment loss, mail at shutdown, no stale writes and
no child cancellation when a parent releases. This extends the current examples;
their explicit per-command exits demonstrate only the release policy.
