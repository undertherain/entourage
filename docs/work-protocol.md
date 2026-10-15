# Work protocol — a durable handle between callers and providers

Brainstorm of 2026-10-08 (in Second Brain), moved here 2026-10-10. Clarified from
first principles on **2026-10-14**, with SDK/context direction and a staged build
plan recorded **2026-10-15**. This is a **design draft with local prototypes**,
not a complete protocol implementation. The first-principles section is the
current explanation; older wire and API sketches remain proposals where marked.
The first application, Library proposals, lives in Second Brain:
`~/Projects/Second Brain/_development_tasks/work-protocol.md`.

## Start at the call site

A calls B somewhere in its logic: B may be a plain tool, another agent, or a
service. The first choice is whether A needs B's result before taking its next
logical step. Call these **await** and **continue**. They describe A's control
flow, independently of process residency and result delivery.

| Question | Choices |
|---|---|
| Does A's next logical step require B's result? | await / continue |
| If awaiting, can A release its execution? | block / checkpoint and resume |
| How does A learn about changes? | open connection / mailbox / pull |

Not every combination is supported by every endpoint. The pipe checks what the
two sides can honour when it opens; capability negotiation is still unbuilt.
The current prototype calls await `wait` and continue `connected` when the
result is delivered through A's mailbox.

### Await: the result belongs to the current step

The ordinary LLM tool loop is: model requests tool → tool finishes → append the
tool response → call the model again. A cannot take that next model step until
the response is present. There are two ways to execute the same logic:

- **A cannot checkpoint and resume.** An adapter promising the actual result
  keeps the call open until B finishes, subject to the call's limits. A's process
  or connection remains occupied. Returning a handle instead changes that
  promise and requires A to know how to use it.
- **A can checkpoint and resume.** Record the outstanding call and where to
  continue, then park. When the result arrives, restore state, fill the slot,
  append the tool response, and take the next step. No worker needs to be held
  while waiting; whether a process stays warm is a separate residency policy.

Resuming at arbitrary Python instructions is unnecessary. A known continuation
such as "collect outstanding tool results, then call the model" suffices. The
session core restores explicit state at `resume`, not a saved Python stack.

A strict await can buffer user messages until the tool responds. This keeps the
conversation sequence simple and avoids model turns just to check progress.
An interruptible await may instead be promoted to continue when A needs a model
turn to handle a user message or B's question. Promotion closes the original
tool call with a status response and retains the outstanding work.

### Continue: acceptance now, result later

A starts B and proceeds without B's final result. In an LLM loop, the immediate
tool response acknowledges acceptance and identifies the work: "started as work
X; the result arrives later." That closes the model's tool call. The eventual
result enters as a new conversation event rather than filling an unresolved
tool response. A continuing caller may receive mailbox notifications or retain
a handle and pull later; a mailbox is one delivery choice.

Starting work and registering it for A must be one durable operation before
success is reported. For a locally spawned provider, the prototype commits the
child, request, caller state and panel entry in one checkpoint. Distributed
delivery will need equivalent guarantees through an outbox and deduplication;
it is not implemented yet.

### Outstanding work and the panel

The **panel** is the agent's view of its outstanding work. Its backing registry
tracks both awaited and continuing calls: identity, correlation, status, mode
and available controls. It is not limited to background work. In the prototype
this is the `Panel` table in session state, built on `Exchanges`.

Receiving through a mailbox does not imply continue: an awaiting A can use the
same mailbox to wake and resume its logical step. Conversely, continue does not
require mailbox delivery if A can inspect a durable handle later.

### Correlation and trusted delivery

A must associate an incoming result with its outstanding call. Today
`Exchanges.ingest` matches both `request_id` and the expected `source`. This is
correlation in a trusted local store: a sender name is not an authenticated
identity, and a correlation ID is not proof that B sent the event.

**Proposed responsibility boundary, not yet implemented:** opening a pipe
arranges a return route that only the authorised provider can use. The messaging
or transport layer enforces the sender's authority; the panel checks which
outstanding work the event belongs to. Whether this uses a restricted reply
capability, authenticated worker identity, or key exchange is open. The LLM does
not manage credentials. The same authority checks are needed for controls sent
from A to B.

### Current build status

| Part | Status |
|---|---|
| `ChatAgent` delegation through `Pipe` and `Panel` | prototyped and covered by scripted tests: wait, connected, promotion, controls, failure and restart |
| `Pending`, `Lifted`, `ToolRunner` | shared SDK primitives in `entourage.deferred_tools`; `ChatAgent` strictly awaits pending tool results; bare and SDK demo variants use the same primitives |
| `Pending` underneath `Pipe` | next implementation step; pipes still dispatch separately from ordinary tool outcomes |
| Provider offers and dynamic opening checks | proposed; prototype grants are declared by hand |
| Blocking adapter for a caller that cannot resume | described here; not implemented as a generic protocol adapter |
| Cooperative cancel and caller-side result fence | prototyped; forced cancel after a grace period remains unbuilt |
| Independently attachable work handle | session inspection exists; reattachment and delivery after the original caller is gone remain incomplete |
| Authenticated pipe endpoints, reactive stream, external-caller adapter | unbuilt |

Case A has not been run against a real model. The wire sketch below and its
shared update-log contract are proposals, not guarantees of these prototypes.

### Using deferred tools

Implemented 2026-10-15. A tool keeps `schema` and `execute(**arguments)`. A final
result becomes text (non-string results are JSON-encoded); `Pending(definition,
payload)` asks the caller to spawn and request a provider instead. `ChatAgent`
registers the outstanding call in the existing panel, matching results by
request ID and expected provider. User amendments are buffered until all strict
waits resolve, including when results span input batches or workers restart.

```python
from entourage.deferred_tools import Lifted, ToolRunner
from entourage.executables import Dispatcher, Executable
from entourage.turn import ChatAgent

# store, complete and tool are supplied by the application.
worker = Dispatcher(store)
worker.register(Executable("tool-runner:v1", ToolRunner([tool]).resume),
                lease_seconds=600)
worker.register(Executable("chat:v1", ChatAgent(complete, [Lifted(tool)]).resume))
worker.create("chat", "chat:v1", {})
store.append("chat", {"event_id": "user:1", "kind": "user",
                      "payload": {"text": "Use the tool to investigate this."}})
worker.run_until_idle()
```

`Lifted(tool, runner="tool-runner:v1")` is an adapter; the registered runner owns
the actual tool instances. A custom tool can return `Pending` directly to target
another provider definition. Providers receive a `request` and must reply with
`payload.text` through `context.reply`; terminal `status` defaults to completed.
Reported failure/cancellation is a visible outcome. A caller-side cancellation
fence still discards late results for work A cancelled itself.

`ToolRunner` runs one plain tool and completes. Lookup, execution and result
encoding errors become tool-result text, matching inline execution. A runner
tool must return a final result; nested `Pending` becomes a tool error. The runner
rejects unexpected mail rather than silently consuming it. Lifted tools offer
no interactive controls in this slice.

Lifting releases A's activation; B must still finish within its own lease. There
is no lease renewal or forced kill. External side effects still require retry-safe
operation identities or reconciliation. As with pipes, a provider that exhausts
its attempts needs a runner failure notice delivered to A; a bare `Dispatcher`
does not publish those notices automatically. Use the shard's notification route
or `runner.notify_failures(store, caller_session)` for the local proof.

Run `python examples/tool_loop.py --chat-agent` for the scripted SDK scenario;
omit the flag for the bare-loop explanation. Both use the shared deferred-tool
implementation. Validation is recorded under *Next*.

## Agent SDK direction (2026-10-15)

The intended SDK is the reusable core of an interactive, tool-using agent:
model loop, mailbox ingestion at safe points, pipes, outstanding-work panel and
context lifecycle. Coding is one application, supplying tools, prompts and
policies; CLI and chat are interfaces to it. The execution runtime supplies
checkpoints, durable delivery, leases, retries and workers underneath.

Two representative cases keep the design grounded:

| Case | Smallest useful behaviour |
|---|---|
| Autonomous automation | ordinary model/tool loop, awaited results, no application-facing mailbox or panel required |
| Interactive assistant | user amendments during work, await or continue per call, questions and results arriving later, panel controls |

Shared contracts and invariants should cover both; optional policies supply
the richer behaviour. A durable runner may use mail internally without making
an automation author manage it. No universal context strategy is required.

### Snapshot/resume with outstanding work

A checkpoints its own state, continuation and outstanding requests. B progresses
independently and sends a result to A's durable inbox. A incorporates that result
and acknowledges the input in the same checkpoint. A simultaneous snapshot of
all agents is unnecessary for message-based coordination.

Local atomic publication, stable operation identities, durable inboxes and lease
fencing supply the foundation. Distributed delivery remains future work. Task
revision is a proposed addition: a result may be correctly delivered but based
on an earlier direction. Keep that provenance so A can decide its relevance;
an amendment does not automatically cancel all earlier work.

A crash during a black-box external operation still leaves an uncertain outcome
if the operation succeeded before its result was committed. Snapshot recovery
does not solve that: use an idempotency key, receipt/status lookup, or an explicit
unknown outcome. Session-commit fencing does not fence external side effects.

### Current direction and composable context

Keep three representations distinct:

- **Event history:** what arrived and happened, with source and provenance.
- **Working state:** current objective, constraints, decisions and outstanding work.
- **Model context:** a bounded, linear projection for the next model decision.

User input may amend working state instead of merely extending a transcript.
For example, "investigate A and B", then "leave B for later" yields an active
objective A and deferred work B. Preserve the originating input; do not silently
discard qualifications or guess at ambiguous amendments. Interpreting arbitrary
natural-language amendments is a separate policy and may require model judgment.

The model's input is linear; the working state need not be. Proposed replaceable
policies cover input interpretation, context assembly and compaction. One owner
makes the final context decision; other capabilities contribute facts, tools or
evidence. Tool-call/result validity, source attribution and provider continuation
requirements constrain every projection.

There is an existing seed in `entourage.capabilities`: contributive capabilities,
one `ConversationLifecycle`, and `TurnPlan.view` separate from stored history.
These hooks belong to earlier drivers and are not wired into the current
`ChatAgent`. Reuse their ownership principle; their exact API is not yet chosen
for the new loop. Earlier context/history discussion is in
[conversation mailboxes](conversation-mailboxes.md#three-independent-histories).

**Cache-aware projection is an experiment, not a default guarantee.** Appending
an amendment preserves an existing prefix; rewriting an earlier block can lose
reuse after that point. A shorter prompt is not necessarily cheaper. Candidate
layout: stable instructions/tools → stable task checkpoint → recent valid
exchanges → latest direction and relevant new information. Consolidate amendments
periodically rather than rewriting the early checkpoint every call. Measure
cache hits, uncached input, latency and output quality; provider rules differ.
See [Claude prompt-cache diagnostics](https://platform.claude.com/docs/en/build-with-claude/cache-diagnostics).

### Temporal comparison

Temporal overlaps substantially with the runtime and supports agent frameworks;
it is a useful baseline. The user's preference is explicit snapshot/resume over
history replay. Nexus is worth studying before extending our wire protocol:
it already models inline results, ongoing operations, callbacks and cancellation.
This does not commit us to a Temporal backend or Nexus adoption. The SDK's loop,
context and interaction policies remain a distinct layer. References:
[Workflow replay](https://docs.temporal.io/workflows),
[agent integrations](https://docs.temporal.io/integrations),
[Nexus specification](https://github.com/nexus-rpc/api/blob/main/SPEC.md).

## Why

Every interaction between a caller A and a provider B takes one of three shapes,
and today each consumer picks one per case:

1. **RPC**: A calls B and gets the result once (a tool call).
2. **Callback**: A calls B and B later wakes A (a child replying to its parent's
   `reply_to`; a router appending to a mailbox).
3. **Durable handle**: A calls B, gets an ID, and inspects it later from any
   session (Second Brain's `my_proposals`, `read_people_intake`).

The question that started it: should an MCP provider behave like a subagent the
caller can always check? The answer we arrived at: the handle is the primitive,
and the other two are ways of *attaching* to it. What the caller is (a tool, an
agent, a terminal session) and what the provider is (code, an agent, a human in
the loop) does not change the contract.

Two facts shaped it:

- MCP is no longer plain RPC. Its 2025-11-25 revision added an experimental
  Tasks utility: a task-augmented request returns a task handle instead of the
  output; the caller polls `tasks/get`, fetches `tasks/result`, may
  `tasks/cancel`; statuses include working, input_required, completed, failed,
  cancelled. It is poll-only, one-directional and experimental. The protocol
  should render as an MCP task, not compete with it.
- Codex follows a running tool by reading the terminal's stdout. That is a
  degenerate attachment (live connection, text rendering, no address), and it
  works. A cleaner abstraction must keep that case trivial.

## The shape

**One state machine, three attachments.** The handle is mandatory. How A learns
about change is negotiated from what A can receive, never assumed by B.

| Attachment | A supplies | Carries | Survives A going away? |
|---|---|---|---|
| **Pull** | nothing | full state, result | yes: it *is* the handle |
| **Reactive** | a live connection | progress, payload allowed | no: ephemeral |
| **Proactive** | a durable address (a mailbox) | a reference to the handle | yes |

Semantics never change across rows; only delivery does. The terminal stream is
the reactive row rendered as text. When that session dies the handle still exists
and the next session resumes by pull.

**Two-sided.** A-to-B has primitives too: *cancel*, *steer* (append a message to
the work), *answer* (supply what B asked for). The `input_required` state is where
this bites: B saying "I need a human" or "I need A". The protocol names that
direction instead of letting each provider invent it.

**Addresses, not connections.** An attachment end is an address A hands over.
Agents have one already: their session's mailbox. External callers (Claude Code,
Codex) have none and get pull. B never derives A's address from the connection.

**References outward, payloads inward.** Proactive delivery carries a reference to
the handle. A reactive stream may carry payload because nothing durable depends on
it.

**Interface, not a wrapper kind.** The protocol says what a handle must expose; a
provider's own record conforms to it. There is no central task table.

- owner principal (who may attach, steer, cancel)
- status, from a shared vocabulary with `input_required` and terminal states
- revision (what A read when it last acted; stale actions are refused)
- correlation key (what A uses to recognise B's answer)
- updates: what changed, when, by whom, with a replayable log

## How it maps onto the session core

An Entourage session already is most of a handle: an ID, durable state, its own
mailbox, a revision, readable from the store without consuming anything.

| Protocol part | Session core today |
|---|---|
| handle | a session: `inspect(session_id)` returns state, status, revision, attempts, last error |
| pull | `inspect` and `list_sessions`; reading never takes a lease or consumes mail |
| proactive attachment | `Context.request` stamps `reply_to`; `Context.reply` delivers to it; `runner.notify_failures` posts to a notify mailbox |
| callback between agents | `Context.spawn` + `Exchanges.call`: child and request in one checkpoint, reply matched on `request_id` and sender |
| correlation key | `request_id` (the child key for spawned work) |
| steer | `append` to the session: always-interruptible, the handler sees it at the next wake |
| answer | an ordinary reply event; the handler matches it through `Exchanges.ingest` |
| replayable update log | retained mail and the current snapshot exist; no general update log, past-checkpoint replay or cross-session feed |

Gaps, in order of how soon a consumer will hit them:

1. **Work status is not session status.** `ready/active/waiting/complete/failed`
   is *scheduling* status. `input_required`, `cancelled` and `expired` are about
   the work, and live in application state today. The protocol needs the work
   status readable without knowing each definition's state layout. The
   `ChatAgent` prototype uses the reserved key `state["work"]`; a shared contract
   for other providers remains to be defined.
2. **No forced cancel.** Parking and completion cancel nothing, by design
   (children and outstanding requests are independent). The prototype sends
   cancel mail, honours it in `ChatAgent`, and fences late results at the caller.
   Runtime enforcement after a grace period and propagation to children are open.
3. **No owner principal.** The store is a trusted local API; senders are names,
   not credentials. Who may attach, steer or cancel is the deployment's policy
   (Second Brain's toolplane), not the session core's.
4. **No revision-checked steering.** `append` does not take an expected revision,
   so A cannot say "this answer is for the question at revision 7". Exchanges
   recover it per request; a generic form would be a conditional append.
5. **No reactive attachment.** There is no live stream out of an activation.
   Progress for an external caller would be published by the provider as mail or
   rendered by the edge (MCP progress notifications, a terminal).

## Wire sketch (proposed 2026-10-10, not agreed)

Goal: an MCP task and a plain tool call are both cases of one call. MCP lets
the *caller* choose up front: a plain `tools/call`, or a task-augmented one that
always returns a handle and never the result. Nothing in between.

```
start(op, input, key, wait, reply_to?) -> Done(result, handle) | Pending(handle, status)
read(handle, after=cursor, wait=, until=terminal|any)    # pull and long-poll in one verb
send(handle, kind=answer|steer|cancel, body, if_revision=)
```

- **The provider decides at return time, within the caller's wait budget.** If the
  work reaches a final state within `wait`, the result comes back inline;
  otherwise the caller gets a handle. A plain tool call is `wait` = transport
  timeout: a handle created and finished in one round trip, its ID never needed.
  An MCP task is `wait` = 0. An Entourage `Context.request` is `wait` = 0 with
  `reply_to`. Precedents: HTTP `Prefer: respond-async, wait=N` with `202
  Accepted` (RFC 7240); Google long-running operations (AIP-151).
- **Every attachment is a cursor over the handle's log.** Log entries: accepted,
  progress, needs (a question), answered, steered, result, failed, cancelled.
  Status is computed from the log.

| Rendering | Is |
|---|---|
| `tools/call` | `start(wait=∞)`, then a blocking `read(until=terminal)` in the same round trip |
| `tools/call` + `task` | `start(wait=0)` |
| `tasks/get` | `read(wait=0)`, a snapshot |
| `tasks/result` | `read(wait=∞, until=terminal)`: a plain call re-attaching to its handle |
| `notifications/progress` | a live push of progress entries |
| `tasks/cancel` / elicitation response | `send(cancel)` / `send(answer)` |
| mailbox `reply_to` | a durable push carrying a reference to the handle |
| terminal stdout | a live push rendered as text |

- **A timeout becomes a handle instead of an error**, with a model-readable line
  (MCP's `io.modelcontextprotocol/model-immediate-response` is that slot). **A
  retry re-attaches instead of duplicating**: `start` carries an idempotency key.
- **Deliver inline if attached, otherwise park on the handle**, for results,
  questions (elicitation if the caller can answer live, else `input_required`)
  and progress alike.
- **Levels.** L0: synchronous only, the handle lives only for the call (every
  plain tool today). L1: durable handle and `read`, so a timeout can become a
  handle. L2: adds needs/answer, cancel, steer. L3: adds push (live progress,
  mailbox).
- **Lifter.** An L0 provider cannot outlive its caller's connection (a stdio MCP
  server dies with its client). Running the call inside a detached Entourage
  session lifts it: the session is the handle. In Second Brain the toolplane
  already leaves a receipt per attempt; the receipt becomes the handle, and an L0
  call's receipt is created already finished.
- **Departures from MCP:** the provider decides within a budget (MCP "required"
  is a provider that always returns `Pending`, "forbidden" is L0); steering; push
  to durable addresses; `if_revision` against stale answers. Shared with MCP:
  cancel is a status, not a kill (MCP keeps `cancelled` even if execution goes on;
  Second Brain's `modules/work` fences the same way).

## The pipe: declared when it is created (in discussion, 2026-10-10)

This section preserves the original discussion and its wait/connect vocabulary.
The current explanation above separates those logical modes from delivery and
execution policy. The opening checks below describe the intended contract.

**User direction.** Get both worlds by opting in when the pipe between A and B is
created, the way C, Go and Rust force a caller to check an error value, or a
function declares whether it can raise. Both sides learn the pipe's settings.
For the LLM loop calling tools (case A), as stated:

- a selector per call: **wait for the result**, or **connect to my mailbox**;
- under wait: **can sleep** on the tool call, or **can't sleep** (recalls the
  wait-on-mailbox semantics);
- under mailbox: no waiting, so sleep or not does not apply; connecting is like
  registering the work in the caller's current panel, which exposes steer and the
  other A-to-B controls;
- all over one pipe that enables or disables these, and tells the other side.
- Combine it with the resume helper.

**Proposed synthesis (not agreed).**

- *Provider declares per operation* what it may produce: `pend: never | maybe |
  always` (MCP's `execution.taskSupport` forbidden / optional / required),
  `ask: never | maybe`, progress, and whether it accepts steer and cancel.
- *Caller declares a handler per outcome* when it binds. The pipe checks the two
  at creation, like checked exceptions at compile time. A mismatch is resolved
  there (the pipe blocks for a caller that cannot take `Pending` but can sleep)
  or the bind fails; it is never discovered mid-call.
  - Pend: **block** (hold the connection or process up to a budget), **pull**
    (the caller gets the handle and reads later), **mailbox** (the work is
    registered with the caller; its updates arrive as mail).
  - Ask: **answer**, **propagate** to my own caller (re-raise), **refuse** (the
    provider uses a default or fails).
  - Progress: drop, stream, mail.
- This is algebraic effects with durable continuations: the provider performs
  Pend or Ask, the caller's handler decides, and a parked session is the captured
  continuation. **The resume helper is the handler runtime.**

**Agreed 2026-10-10 (user):**

- **A capability the other side cannot honour is a type error at pipe creation.**
  A pipe that allows steering requires the provider to attach the work to its own
  mailbox; a provider without one cannot offer steer. Each capability names the
  endpoint it requires:

| Capability | Requires | Without it |
|---|---|---|
| steer | provider has a mailbox and reads it while working (interruptible) | type error |
| cancel | provider-side handle; a lifter's session suffices (cancel fences, no kill) | type error |
| answer (Ask) | caller has a live connection (elicitation), a mailbox, or a pipe upward that allows Ask | caller must declare refuse |
| mailbox delivery | caller has a mailbox | type error |
| block | caller holds a live connection or process | n/a: always possible |
| pull | provider keeps a durable handle (L1) | type error |

  A lifted L0 tool (plain function in a wrapper session) can therefore offer
  cancel but never steer: the wrapper has a mailbox, the function does not read it.
- **Sleep is the default only when the caller has no mailbox.** Without a mailbox
  the caller can block or pull. With a mailbox the default is to connect it; to
  wait anyway is not a pipe mode but the existing wake condition on the caller's
  own mailbox (completion `all` on that reply, interruptible or not,
  [mailbox-first scheduling](mailbox-first-scheduling.md) reframe 5), and it holds
  no process. Whether a process stays warm is residency
  ([executable lifecycle](executable-lifecycle.md)), not protocol.
- **The provider owns honouring what it declares.** Declaring "I implement
  steer" obliges the provider; the default implementation delivers all steering
  to its own mailbox, where the handler sees it at the next wake.
- **The caller's logic picks per call; whether it may is a pipe option.** The
  pipe declares the allowed modes (wait, connect) and whether the choice is per
  call. Within that, A's own logic decides each call: code in a workflow, the
  model in an LLM loop (a tool argument, like `run_in_background`).
- **Panel = the agent's registry of the work it called** (subagents, background
  tool calls). Connecting through the mailbox registers the work there; a panel
  entry is what exposes steer, cancel and answer to the agent. `entourage.exchanges`
  is its seed: the pending-exchange table already in state, extended with handle,
  status and the pipe's capabilities.
- *Combined with the resume helper,* a `ChatAgent` tool call through the pipe:
  Done appends the tool result; Pend with a wait condition records the exchange
  and parks; on wake the helper folds the reply in as the tool result, so the
  model cannot tell a parked call from an inline one (Go's model: calls look
  blocking, the runtime parks cheaply). Pend without one leaves a panel entry and
  the model goes on; the reply arrives later as mail.
- *Precedent:* Claude Code's `run_in_background` per call, completion as a
  notification, a task panel that lists and stops running work.

**Deferred.** Ask across a chain (Concierge → agent A → tool B, B asks something
A cannot answer): leave it to the LLM and B until a real case needs a rule.

## Case A worked through: `ChatAgent` with pipes (proposed 2026-10-10)

Originally checked against `entourage/turn.py`, `entourage/exchanges.py` and
`Context` in `entourage/executables.py` as of `4e83479`. The later prototype
implements the loop and panel behaviour described below; offers and negotiated
opening remain proposed. Code snippets in *Declarations* sketch the intended
API, not the current API.

### Declarations

**Provider: an offer, persisted in the definition contract.** A session-backed
provider declares it at registration; it goes into the JSON contract that
`bind_definition` already persists, so a caller in another worker can check it
from the store without loading the code.

```python
Executable("research:v1", resume, offers=Offer(steer=True, cancel=True, ask=True))
# contract: {..., "offers": {"pend": "always", "steer": true, "cancel": true,
#                            "ask": true, "progress": false}}
```

A session provider is always `pend: always` (it replies in a later activation).
An inline tool (`schema` + `execute`) is `pend: never` with nothing else: L0.

**Caller: a pipe, opened at runtime.** Pipes are dynamic (user, 2026-10-10): the
master agent finds a fitting subagent on the mesh and delegates to it. So the
check happens when the pipe is opened, before any work is sent, not when code is
registered. Like Go's `v, ok := x.(Steerable)`, opening returns a pipe or a refusal,
and the caller must handle the refusal.

```python
pipe, refusal = panel.open("research:v1", modes=("wait", "connected"),
                           want={"steer": "required", "cancel": "optional"},
                           ask="answer")
```

- The provider's offer is read from the store (the persisted contract), so opening
  costs no round trip to the provider.
- Each wanted capability is `required` (missing → refusal) or `optional` (missing →
  the pipe opens without it). The opened pipe says what was granted, and the
  request mail carries it, so the provider knows what the caller can handle (the
  "inform the other side" part).
- An offer with `ask: true` and a caller with no Ask handler: refusal. `refuse` is
  an explicit handler.
- `connect` or a wait budget towards an inline tool: refusal, unless the caller
  asks for `lift`, which runs the tool in a generic tool-runner child session
  (cancel then works as a fence; steer never does).
- A pipe fixed in code (`ChatAgent(pipes=[...])`) is just a pipe opened at start.

Discovery itself (how the master searches the mesh) is outside the protocol. The
protocol only needs every agent to publish its offer where a searcher can read it.

### Mail kinds on a pipe

A to B: `request` (with `reply_to`, exists today), `steer`, `cancel`, `answer`.
B to A, each carrying `request_id`: `progress`, `ask`, and `result` with a final
status `completed | failed | cancelled`. The prototype supports these mail kinds;
`progress` is retained on the panel entry without being shown to the model.

### What the model sees

- Proposed dynamic tools: `find_agents(query)` returns candidates with their offers;
  `delegate(agent, task, background?)` opens the pipe and sends the request in one
  step, and a refusal comes back as the tool result. A fixed pipe can still appear
  as its own tool.
- Proposed: panel tools appear only for capabilities some pipe actually offers:
  `steer(work, text)`, `cancel(work)`, `answer(work, text)`. A capability the
  pipe lacks is rejected for that work. The prototype exposes tools from the
  union of manually declared pipe grants; provider verification and per-work
  capability enforcement remain to be built.

### The panel entry

`Exchanges` grows into the panel: the same table in state, keyed by `request_id`.
This sketches the intended entry, including negotiated offers that the prototype
does not yet store.

```json
"panel": {"<request_id>": {"to": "<child session>", "label": "research",
                           "tool_call_id": "call_1", "mode": "wait",
                           "status": "working", "question": null,
                           "offers": ["steer", "cancel", "answer"]}}
```

### One activation, step by step

1. Ingest: `panel.ingest(mail)` first. `result` for a `wait` entry becomes the
   deferred `role: tool` message for its `tool_call_id`; `result` for a
   `connected` entry becomes a message to the model ("[research finished] …");
   `ask` sets `input_required` and the question; everything else is ordinary mail,
   as today.
2. A tool call on a pipe: `Exchanges.call` (spawn plus request, one checkpoint)
   and a panel entry.
   - **connect**: append the tool result at once, a status line ("started as work
     X; the result arrives as a message"). The loop goes on.
   - **wait**: append nothing yet. The model cannot be called while a tool call
     has no result (the chat-completions message rule), so the session parks
     until every waited reply is in, or the wait budget's deadline passes.
3. Panel tools: `steer` and `answer` send to the child; `cancel` sends `cancel`
   and marks the entry `cancelled` at once. The entry is kept as a fence until
   the final result arrives, so that result is matched and discarded rather
   than reaching the strict unknown-mail check. This does not force B to stop.

### Finding: wait is connect with the tool result deferred

The two modes differ only in *when the tool result is written*. **Promotion**
writes it early, as a status line, and turns the entry from `wait` into
`connected`. Three things promote:

- the wait budget passes (the wire sketch's "a timeout becomes a handle");
- the work asks a question: the question *is* the early tool result ("work X
  needs: which account? reply with `answer`"), since the model needs a turn to
  answer;
- the user writes while the agent waits, if the pipe is interruptible; otherwise
  the message is buffered in state until the waits resolve (the strict join of
  [mailbox-first scheduling](mailbox-first-scheduling.md)).

So promotion is one mechanism serving timeout, Ask and interruption, and the model
never sees a dangling tool call.

### What it takes to build

**Items 1 to 3 are built (2026-10-10, prototype):** `entourage/panel.py` is
`Exchanges` grown into the panel; `ChatAgent` takes `pipes=[Pipe(...)]`, serves
requests as a provider, and `tests/test_pipes.py` covers wait, connected,
promotion by interruption, budget and question, cancel as a fence, a failed
worker, and a fresh worker resuming a parked wait from the store. Decided while
building: the provider's work status lives under the reserved state key
`state["work"]` (`status`, and the `request` it answers), so `inspect` reads it
without knowing the definition; `Exchanges.ingest` reports `ask` and `progress`
without closing the exchange; `Context.send` takes `request_id` for mail on an
open exchange. Ask is implemented as promotion only (the question becomes the
tool result); `progress` is stored on the entry and not shown.

The list as written before building, kept as the record: `ChatAgent`
is Entourage's reference agent loop (`entourage/turn.py`, used by
`examples/cli.py` and `coding_agent.py`); Second Brain's Concierge has its own
`resume` and only borrows `litellm_complete`, so it would adopt the panel
separately.

1. **`ChatAgent` cannot receive a reply.** When it wakes, it goes through its new
   mail and accepts only user messages and timer ticks. Anything else makes it
   stop with an error, on purpose, so unknown mail is never silently dropped
   (`turn.py`, the `raise ValueError("unexpected mail …")` line). A subagent's
   answer arrives as mail of kind `result`, so today it would crash the agent's
   turn; after three tries the session is marked failed. The strict rule stays;
   this is the panel-mailbox pairing: the panel reads the mail first and takes
   the pipe's kinds (`result`, `ask`, `progress`), and the rest reaches the agent
   as before.
2. **`Exchanges` only understands final answers.** It recognises mail of kind
   `result` and then forgets the request. A question or a progress note from the
   subagent would not be recognised as belonging to that request.
3. **`ChatAgent` cannot be the subagent.** It does not accept a `request` (that
   would also hit the error in point 1), and when it finishes it sends the answer
   to its fixed `output` address instead of back to whoever asked. It has no
   notion of `steer` mail either.
4. **Agents publish no offers.** The saved contract of a definition has no place
   for "I accept steer, cancel, questions", so nothing can be checked or searched.
5. **Plain tools cannot be lifted.** There is no generic session that runs a plain
   tool in the background, so a plain tool can only run inside the agent's turn.
6. **A plain tool must finish within the turn's time limit.** The lease is the hard
   limit for one turn (30 s by default, never extended). A tool that runs longer
   kills the turn. That is why long tools need a pipe at all.

## The loop underneath (2026-10-10, evening)

Before the pipe, the user asked for the bare case: "read the user's message,
call the model, run tools if it asks, repeat until it answers, then wait for the
next message", written without `ChatAgent` or the panel, to see what
durability adds. `examples/tool_loop.py` is that loop, with
`tests/test_tool_loop.py`. Findings:

- The three places to continue from are three methods: `call_model` when a
  user message or a tool result is in, `call_tools` then exit, `ask_user` then
  exit. `resume` reads the mail and picks the place. That is the "little bit of
  graph" the user missed from Entourage 1, as a phase table inside the handler.
- Durability adds one thing: the **slot**. A tool may return `Pending`
  ("run this elsewhere": a definition and a payload) instead of a result; the
  loop spawns it, keeps the tool call open and parks. The result comes back as
  mail and fills the slot. `Lifted(tool)` turns any plain tool into one that
  returns `Pending`, and `ToolRunner` is the session that runs it under its own
  lease. That is build item 5 in its minimal form, and the degenerate pipe.
- The second thing it adds is forced by the chat format: a user message that
  arrives while a slot is open is **held** in state and appended after the
  result. This is the strict wait of `ChatAgent`; the mailbox is only the
  delivery, nothing of it shows in the loop.
- Exit after calling tools is a `propose` with `deadline=NOW` when every result
  is in, so the next activation continues at once with a checkpoint between.
  That checkpoint is the window in which a user message lands between a tool
  result and the next model call.

Direction from this (user): start from this loop, not from the pipe. `Pending`
as a tool return should become the primitive under `Pipe`, so `ChatAgent` stops
knowing which tools are pipes; the panel is then the registry of open slots
with modes and controls on top.

## Next (where to pick up)

**Build one small proof in two modes before expanding the framework.** A asks B
to compare two storage options; the user defers the second while work is
outstanding; the caller restarts; the result arrives; the next model decision sees the amendment
and the result with its provenance. Exercise strict await and continue. The
strict version buffers the amendment until the slot fills; the interactive
version closes the tool call with an acknowledgment and handles the amendment
while work remains outstanding.

This is the implementation sequence. Each step should leave a working
example and a reviewable change, using the existing session runtime.

1. **Done 2026-10-15: share the deferred-tool primitive.** Extract `Pending`, `Lifted` and
   `ToolRunner` from `examples/tool_loop.py` into the SDK. Teach `ChatAgent` to
   handle an immediate result or `Pending` through one outcome path, initially
   with strict await. Record expected provider as well as request/tool-call IDs
   and surface provider failure as a tool outcome. Proof: inline and lifted
   versions of the same tool, a correction during the wait, multiple open calls,
   and a fresh worker recovering the saved slots. Reuse existing fixtures.
   Built in `entourage.deferred_tools` and integrated into `ChatAgent`; the
   existing panel tracks strict waits. The SDK demo is `tool_loop.py --chat-agent`.
   Focused tests cover parity of inline/lifted outcomes, multiple calls, bounded
   batches, provider matching, failure notices and actual subprocess recovery.
   Validation: **31 passed** across `test_turn.py`, `test_deferred_tools.py`,
   `test_tool_loop.py` and `test_pipes.py`; full repository regression
   **312 passed, 132 skipped**. Commands use
   `PYTHONDONTWRITEBYTECODE=1 python3 -m pytest -q -p no:cacheprovider`, with the
   four test paths for the focused run and no paths for the full run.
2. **Put pipes and the panel on that path.** Make `Pipe` produce deferred work
   through the shared mechanism; keep wait/connected, promotion, questions and
   controls working. Commit registration with dispatch. Record and enforce
   controls for each work item, using explicit grants until offers are built.
   Proof: run the existing pipe scenarios through the shared path, including
   restart, strict buffering, promotion and late-result cancellation fencing.
3. **Add one context-policy seam and one direction example.** Keep transcript
   assembly as the default. Allow one context owner to project working state
   and valid recent exchanges at the next model boundary. Use an explicit,
   structured amendment in the example to update objective/constraints and a
   task revision; retain the originating input and the revision under which
   work was dispatched. Proof: the same amendment/restart scenario with default
   transcript assembly and with a current-direction projection. No automatic
   natural-language interpretation or compaction framework in this step.
4. **Try the slice with a real model.** Use two agents and an inspectable trace
   of the actual model inputs, state, outstanding work and outcomes. Check that
   acknowledgments, questions and amendments lead to sensible decisions.
   Compare append-only and projected context using provider usage/cache metrics
   and answer quality. This experiment informs defaults; shorter input alone
   is not evidence of lower cost.

**Immediate next code change: step 2.** The shared deferred-tool primitive is
built; pipes still have their own dispatch path. The remaining sequence integrates
that path and makes the context seam concrete.

After that slice, without a fixed order:

- **Offers and opening checks** (build item 4 and the proposed opening refusal):
   today a `Pipe` declares `grants` by hand and nothing verifies the provider.
   Enforce granted controls for each work item as well as at opening.
- **Forced cancel** at the runtime (gap 2): cancel mail first, a grace period,
   then stop further activations and fence an in-flight activation's commit.
   Stopping the running computation also needs execution-backend support.
- **Independent handle lifetime:** allow another session to attach and inspect
   work after the original caller is gone; define what happens to delivery to
   the old address. A durable panel entry alone does not provide this.
- **Trusted return routes:** define endpoint authority and where it is enforced;
   do not treat matching `source` and `request_id` as authentication.
- **Case B, external callers** (Claude Code, Codex over MCP). Without a mailbox,
   block or pull apply, with "a timeout becomes a handle" from the wire
   sketch, and `work_read` / `work_send` as the generic tools where a client lacks
   MCP Tasks. Not yet discussed beyond that.
- **Richer context policies:** natural-language direction updates, automatic
   compaction and cache-aware layout tuning, guided by the small experiment.

## Open

- Shared status vocabulary: decided 2026-10-10 to include MCP's (working,
  input_required, completed, failed, cancelled) and add our own as needed
  (`expired`, and whatever the prototype shows). Open: the exact list.
- Forced cancel: how the runtime marks a session cancelled so it never wakes
  again, fences commits and stops a running activation (gap 2). Needed for a
  provider that does not read cancel mail, such as a lifted plain tool.
- Endpoint authority: restricted reply capabilities, authenticated identities,
  or key exchange; the proposed responsibility boundary is in *Correlation and
  trusted delivery*. The store is currently trusted and local.
- Where the interface lives as code: here (it is coordination, and Second Brain
  depends on Entourage), or in Second Brain's core next to the dataplane, whose
  records (intake, proposal, `modules/work` tasks) are handles without being
  sessions. Decide when a second conforming kind needs shared code.
- Opt-in to proactive delivery: on the handle (the caller's intent) or on the
  route (the deployment's permission)? Both can hold.
- A caller that attaches reactively and disappears: does B keep progress anywhere,
  or is reactive strictly best-effort? Lean best-effort; the durable log is the
  record.

## Discussion log

Short and dated, so ideas are not proposed twice. Detail lives in the sections
above. This is a historical record: later entries and the current explanation
supersede earlier proposals where they differ.

- **2026-10-15 (implementation):** extracted `Pending`, `Lifted`, `ToolRunner`
  and tool-result conversion into `entourage.deferred_tools`. `ChatAgent` now
  strictly awaits ordinary tools returning `Pending`, using its existing panel
  and buffered-message handling. The bare demo imports the shared implementation;
  `--chat-agent` exercises the SDK. Added mixed-call, bounded-batch, provider-match,
  failure and fresh-process recovery coverage. Corrected provider-reported
  cancellation handling so it resolves a wait; the caller's own cancellation
  fence continues to discard late results. Step 2 is next.

- **2026-10-15**: clarified the product as a reusable agent harness over the
  execution runtime, with autonomous awaited automation and an interactive
  assistant as the two representative cases. Snapshot/resume tracks local
  state and outstanding messages; globally simultaneous snapshots are unnecessary
  for this ownership model. Black-box side effects retain outcome uncertainty.
  User input can update current direction; event history, working state and
  linear model context are separate. Context policies should be replaceable,
  with one final owner and a simple transcript default. Cache reuse trades off
  against rewriting and compaction. User requested logging and a concrete build
  sequence: shared `Pending` → pipes/panel → one context seam → real-model proof.
  Broader protocol and context features remain later work.

- **2026-10-14**: first-principles discussion distinguishes logical await/continue,
  execution blocking/checkpointing, and connection/mailbox/pull delivery. Known
  continuations suffice; outstanding-work tracking applies to both modes.
  Starting work and recording it belong in one durable operation. Endpoint
  authentication raised as an open design question; matching a sender name and
  correlation ID is not proof of origin. Documentation reconciled with the
  prototypes; `Pending` underneath pipes is the immediate next step.

- **2026-10-08** (Second Brain): handle as the primitive; pull / reactive /
  proactive attachments; documents are files, state is records.
- **2026-10-10**: protocol moved here; the Library application stays in Second
  Brain. Session-core mapping and five gaps recorded.
- **2026-10-10**: wire sketch proposed (wait budget, provider decides, `start` /
  `read` / `send`, timeout becomes handle, retry re-attaches, levels, lifter,
  receipt as handle). Not agreed: the user asked for both worlds via opt-ins at
  pipe creation instead of a single rule.
- **2026-10-10**: pipe direction (user): per-call selector wait / mailbox,
  sleep / can't sleep under wait, mailbox as registering with a panel that exposes
  steer, all over one negotiated pipe, combined with the resume helper. Synthesis
  as checked effects with handlers; open points listed in that section.
- **2026-10-10**: agreed (user): unhonourable capability = type error at pipe
  creation (steer needs the provider's mailbox); sleep is the default only without
  a caller mailbox, with one it is a wake condition; panel = the agent's registry
  of called work. Open: selector per call or per pipe; Ask across a chain.
- **2026-10-10**: agreed: the provider owns honouring its declared interface,
  steering defaults to its mailbox. Ask across a chain deferred (LLM and B work it
  out). Selector: proposed per call within the pipe's allowed set.
- **2026-10-10**: agreed: A's logic picks wait or connect per call; allowing that
  is a pipe option.
- **2026-10-10**: case A worked through against `ChatAgent` and `Exchanges`:
  offers in the persisted definition contract, pipes checked at registration,
  panel as grown `Exchanges`, panel tools shown only for offered capabilities.
  Finding: wait is connect with the tool result deferred; promotion (timeout, ask,
  interruption) writes it early. Six code gaps listed.
- **2026-10-10**: pipes are dynamic (user): the master finds a subagent on the
  mesh and delegates, so the check moves from registration to opening the pipe;
  capabilities wanted as required or optional; fixed pipes are pipes opened at
  start. Discovery is outside the protocol. Gaps rewritten in plain words.
- **2026-10-10**: clarified: the gaps are a build list, not obstacles. The strict
  "unknown mail is an error" rule stays; the panel-mailbox pairing reads mail
  first and takes the pipe's kinds. `ChatAgent` is Entourage's reference loop, not
  Concierge (which has its own `resume` and adopts the panel separately). Next
  steps recorded under *Next*.

- **2026-10-10** (user, evening): the vocabulary includes MCP's statuses but is
  not limited to them. **Cancel is best effort, escalating**: the caller-side
  fence (status flips, late results are ordinary mail) stays; on the provider
  side, cancel mail first, a grace period for a cooperative subagent, then
  forced shutdown for one that does not read its mailbox (a lifted tool, a stuck
  loop). Forced shutdown is a runtime feature (gap 2), now a build item.
  **The pipe should survive both endpoints**: today it is a panel entry in the
  caller plus `reply_to` on the request, durable on both sides but gone with
  either session; the target is the handle readable from any session after the
  caller is gone (the pull row). Clarified in conversation: "wire sketch" means
  the section of that name; "Ask" is the provider's `input_required`; "item 3"
  is `ChatAgent` as subagent. All of it to be adjusted as we prototype.

- **2026-10-10** (prototype): build items 1 to 3 implemented and tested (see
  *What it takes to build*). Findings from building: cancel keeps the panel entry
  as a fence rather than dropping it, so the late result is matched and discarded
  instead of hitting the strict unknown-mail rule; a failure notice closes the
  entry and is the tool result of a wait; a connected result reaches the model
  as a user-role message in brackets (provider support for mid-conversation
  system messages is uneven); the provider completes after one request, so a
  subagent is one job.

Settled elsewhere, do not re-derive: wake-condition knobs and the resume helper
(CbR as a library over state), each session as one stack frame with `reply_to` as
the return address, the subagent pipe whose read end is shared with steering
([mailbox-first scheduling](mailbox-first-scheduling.md), 2026-10-05); no filter
or query surface on the mailbox (2026-08-28); residency policy
([executable lifecycle](executable-lifecycle.md)).

## Sources

- [MCP Tasks, 2025-11-25 specification](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/tasks)
- [MCP Tasks mirror with request flow](https://modelcontextprotocol.net/specification/2025-11-25/basic/utilities/tasks)
- [WorkOS on MCP async tasks](https://workos.com/blog/mcp-async-tasks-ai-agent-workflows)
- [MCP Tasks extension draft](https://tasks.extensions.modelcontextprotocol.io/specification/draft/tasks)
- [RFC 7240, Prefer header for HTTP](https://www.rfc-editor.org/rfc/rfc7240) (`respond-async`, `wait`)
- [Google AIP-151, long-running operations](https://google.aip.dev/151)
- A2A: task states and callbacks are the reference for the two-sided part; its
  transport assumptions are not.
