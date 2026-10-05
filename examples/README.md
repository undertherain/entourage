# Examples

Start with the **[registered mailbox agent](mailboxes/registered/README.md)** for
the current execution API: an adjacent manifest, ordinary `agent.py`, and state
restoration through `Dispatcher` and the SQLite `SessionBackend`. It handles a
user amendment while a tool is pending, then resumes in a fresh process.

The other [mailbox examples](mailboxes/README.md) retain the earlier teaching
prototypes: a receive loop, saved-function continuation and explicit request/reply
driver. Their helpers are example code, not the public executable API.

The other examples all run on the same dispatcher and SQLite store:

| Area | Examples |
| --- | --- |
| Interactive agents | [cli.py](cli.py), [coding_agent.py](coding_agent.py): one model call per activation, tools inline, the conversation is session state |
| Transports | [telegram_group_manager.py](telegram_group_manager.py): one session per chat, triage and answer as checkpointed phases, an outbox session for delivery |
| Children and waiting | [spawn_supervisor.py](spawn_supervisor.py): fork-join, supervisor join with a death notice, impatience; [waiting_session.py](waiting_session.py): the three wake sources |
| Shard runner | [shard/](shard/): agent folders launched and supervised by `python -m entourage.runner` |
| Older graph runtime | [remote_tool_ingress.py](remote_tool_ingress.py), [retry_timeout.py](retry_timeout.py), [mailbox_cli.py](mailbox_cli.py): in-memory graph backends, kept until the sweep in [mailbox-first scheduling](../docs/mailbox-first-scheduling.md#migration) |

Run them from the repository root, for example `python examples/spawn_supervisor.py`.
The chat examples take `--model` for your configured provider; `--help` makes no
model call. The chat and Telegram examples need API keys; the others need nothing.
