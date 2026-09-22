# Mailbox agents

Start with the two small author-facing examples. Run commands from the repository
root; no model keys or external services are needed.

For the implemented registration/dispatcher API, use the
[registered agent](registered/README.md): adjacent manifest, prompt and tools,
correction while waiting for a reply, and recovery across fresh processes.

| Example | What to read |
| --- | --- |
| [resident.py](resident.py) | Receive mail → dispatch tool → handle other mail → ingest result |
| [resumable.py](resumable.py) | Do work → save state and next step → exit → resume in a fresh process |
| [support.py](support.py) | Shared demo helpers, SQLite setup, mock tool and command-line plumbing |
| [request_reply.py](request_reply.py) | Longer request/reply and user-clarification walkthrough |
| [wake_session.py](wake_session.py) | Original direct `claim`/`commit` example without helper syntax |

## Keep the main loop running

```bash
python -m examples.mailboxes.resident
```

Expected order:

```text
Dispatched weather; continuing the mailbox loop.
Handled other mail: Also remember to pack a hat.
Ingested tool result: Mock weather for Kyoto: sunny, 24 C.
```

The tool runs in a separate async task and delivers its result as durable mail.
The agent never awaits the tool directly. Each mailbox batch is checkpointed;
the same process keeps receiving batches. Empty-inbox polling yields the event
loop without holding a session lease. “Stay running” does not mean busy spinning.
The demo exits after its first result to make it easy to run and test.

## Save and restart

Use a fresh database path. Every command is an independent process:

```bash
python -m examples.mailboxes.resumable /tmp/resumable-agent.db start
python -m examples.mailboxes.resumable /tmp/resumable-agent.db run   # no reply yet
python -m examples.mailboxes.resumable /tmp/resumable-agent.db show  # itinerary + next=finish
python -m examples.mailboxes.resumable /tmp/resumable-agent.db tool  # deliver mock reply
python -m examples.mailboxes.resumable /tmp/resumable-agent.db run   # restore and finish
python -m examples.mailboxes.resumable /tmp/resumable-agent.db show
```

`prepare` saves the itinerary, pending call identity and the name of `finish` in
one checkpoint with the outgoing request. The later `run` restores that state,
looks up its saved next step and calls `finish` with the tool reply. It does not
rerun `prepare`. This is restart from an explicit checkpoint, not from an arbitrary
instruction or a saved Python stack. Renaming a step requires migrating its saved
name; these examples do not implement definition version migration.

## What the helpers actually provide

- `turn.call(...)` stages local mail with a reply address and stable call ID.
- `turn.save(...)` atomically saves state, publishes staged calls and acknowledges
  the delivered batch. Handle the entire batch before saving; exceptions leave
  it uncommitted for retry after lease expiry.
- `turn.save(next_step=finish)` stores a continuation name; `run_once` dispatches
  to it after restoring state.
- `mailbox` supplies successive leased batches in the same process.

These are **example-only helpers over `LocalSessions`**, not a registered binary
launcher or a completed public SDK. The examples pre-create one agent and one
mock tool, support one outstanding call, and assume the documented input kinds.
They omit deployment, automatic child creation and production supervision.
The tool CLI stands in for a runtime worker; automatic process wakeup is future
work. The core already provides durable readiness and fenced commits.

For multiple trips and a tool that asks the user a question, continue to the
[longer walkthrough](../../docs/mailbox-tool-examples.md).
