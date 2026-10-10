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

**Caller: a pipe, opened at runtime.** Pipes are dynamic (user, 2026-10-10): the
master agent finds a fitting subagent on the mesh and delegates to it. So the
check happens when the pipe is opened, before any work is sent, not when code is
registered. Like Go's `v, ok := x.(Steerable)`, opening returns a pipe or a refusal,
and the caller must handle the refusal.

```python
pipe, refusal = panel.open("research:v1", modes=("wait", "connect"),
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
status `completed | failed | cancelled`. Only `request` and `result` exist now.

### What the model sees

- Dynamic: `find_agents(query)` returns candidates with their offers;
  `delegate(agent, task, background?)` opens the pipe and sends the request in one
  step, and a refusal comes back as the tool result. A fixed pipe can still appear
  as its own tool.
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

## Next (where to pick up)

Case A holds in tests; not yet run against a real model. Candidates:

1. **Run it for real:** an example with two `ChatAgent`s over litellm, to see
   whether the status lines ("started as work …", "work … asks: …") steer a model
   well, and whether `background` as a tool argument is picked sensibly.
2. **Offers and opening checks** (build items 4 and the `panel.open` refusal):
   today a `Pipe` declares `grants` by hand and nothing verifies the provider.
3. **Forced cancel** at the runtime (gap 2): a session marked cancelled that
   never wakes again, for providers that do not read their mailbox. Then a
   grace period between cancel mail and the force.
4. **Lift plain tools** (build item 5): a generic tool-runner session so a long
   plain tool can go through a pipe.
5. **Case B, external callers** (Claude Code, Codex over MCP). They have no mailbox,
   so only block or pull apply, with "a timeout becomes a handle" from the wire
   sketch, and `work_read` / `work_send` as the generic tools where a client lacks
   MCP Tasks. Not yet discussed beyond that.

## Open

- Shared status vocabulary: decided 2026-10-10 to include MCP's (working,
  input_required, completed, failed, cancelled) and add our own as needed
  (`expired`, and whatever the prototype shows). Open: the exact list.
- Forced cancel: how the runtime marks a session cancelled so it never wakes
  again, and kills a running activation (gap 2). Needed for the "dumb subagent"
  case below.
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
