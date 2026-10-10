# Work protocol — a durable handle between callers and providers

Brainstorm of 2026-10-08 (in Second Brain), moved here 2026-10-10 as a **draft**.
Nothing here is built as a protocol; this doc names what the session core already
does and the gaps. Decide against it, not from it. The first application, Library
proposals, lives in Second Brain:
`~/Projects/Second Brain/_development_tasks/work-protocol.md`.

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
| replayable update log | each session's committed mail and revisions; no cross-session feed |

Gaps, in order of how soon a consumer will hit them:

1. **Work status is not session status.** `ready/active/waiting/complete/failed`
   is *scheduling* status. `input_required`, `cancelled` and `expired` are about
   the work, and live in application state today. The protocol needs the work
   status readable without knowing each definition's state layout: a reserved
   state key, or a status the proposal sets.
2. **No cancel.** Parking and completion cancel nothing, by design (children and
   outstanding requests are independent). Cancel today is a steering message the
   handler chooses to honour. Whether the runtime should know a cancelled state,
   and whether it reaches children, is open.
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

Checked against `entourage/turn.py`, `entourage/exchanges.py` and `Context` in
`entourage/executables.py` as of `4e83479`. Nothing here is built.

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

**Caller: a pipe, declared where the agent is built.**

```python
ChatAgent(complete, tools=[clock, search],
          pipes=[Pipe("research:v1", label="research",
                      modes=("wait", "connect"), default="wait", per_call=True,
                      steer=True, cancel=True, ask="answer")])
```

**Checked when the executable is registered** (the in-shard equivalent of pipe
creation), against the target's offer:

- a capability the caller asks for and the offer lacks: error;
- `connect` (or a wait budget) towards an inline tool: error, unless the pipe says
  `lift=True`, which runs the tool in a generic tool-runner child session (cancel
  then works as a fence; steer never does);
- `ask: true` on the offer and no Ask handler on the pipe: error; `refuse` is an
  explicit handler.

### Mail kinds on a pipe

A to B: `request` (with `reply_to`, exists today), `steer`, `cancel`, `answer`.
B to A, each carrying `request_id`: `progress`, `ask`, and `result` with a final
status `completed | failed | cancelled`. Only `request` and `result` exist now.

### What the model sees

- Each pipe is one tool. With `per_call` and both modes, the schema gets a
  `background: bool` argument (the pipe's default applies when omitted).
- Panel tools appear only for capabilities some pipe actually offers:
  `steer(work, text)`, `cancel(work)`, `answer(work, text)`. A capability the
  pipe lacks is never shown, so the model cannot make the type error.

### The panel entry

`Exchanges` grows into the panel: the same table in state, keyed by `request_id`,
with what the protocol needs.

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
3. Panel tools: `steer` and `answer` send to the child; `cancel` sends `cancel`,
   marks the entry `cancelled` at once and `Exchanges.drop`s it, so a late result
   is ordinary mail (MCP's rule: cancelled stays cancelled).

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

### Gaps in the current code

1. `ChatAgent.resume` raises on any mail kind but user, message and timer, so it
   cannot receive replies. Panel ingest has to run first.
2. `Exchanges.ingest` matches only `kind: result` and deletes the entry. The panel
   needs non-terminal kinds (`ask`, `progress`) that keep the entry, plus status.
3. `ChatAgent` cannot be a provider: it does not accept `request` mail, sends its
   answer to `output` instead of `context.reply`, and has no `steer` kind.
4. `Executable` has no offers, and the contract does not carry them.
5. There is no tool-runner definition to lift an inline tool.
6. The lease is the hard step limit (no renewal), so a blocking inline tool longer
   than the lease fails the step. Today that is the only block mode, and the main
   reason long tools need a pipe.

## Open

- Shared status vocabulary: adopt MCP's (working, input_required, completed,
  failed, cancelled) or A2A's richer set? Lean MCP, plus `expired`.
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
above.

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
