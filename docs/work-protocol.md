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

## Sources

- [MCP Tasks, 2025-11-25 specification](https://modelcontextprotocol.io/specification/2025-11-25/basic/utilities/tasks)
- [MCP Tasks mirror with request flow](https://modelcontextprotocol.net/specification/2025-11-25/basic/utilities/tasks)
- [WorkOS on MCP async tasks](https://workos.com/blog/mcp-async-tasks-ai-agent-workflows)
- [MCP Tasks extension draft](https://tasks.extensions.modelcontextprotocol.io/specification/draft/tasks)
- A2A: task states and callbacks are the reference for the two-sided part; its
  transport assumptions are not.
