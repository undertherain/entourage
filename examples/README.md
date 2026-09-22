# Examples

Start with the **[registered mailbox agent](mailboxes/registered/README.md)** for
the current execution API: an adjacent manifest, ordinary `agent.py`, and state
restoration through `Dispatcher` and the SQLite `SessionBackend`. It handles a
user amendment while a tool is pending, then resumes in a fresh process.

The other [mailbox examples](mailboxes/README.md) retain the earlier teaching
prototypes: a receive loop, saved-function continuation and explicit request/reply
driver. Their helpers are example code, not the public executable API.

The existing examples cover these other surfaces:

| Area | Examples |
| --- | --- |
| Interactive agents and transports | [cli.py](cli.py), [coding_agent.py](coding_agent.py), [mailbox_cli.py](mailbox_cli.py), [telegram_group_manager.py](telegram_group_manager.py) |
| Graph waiting and coordination | [waiting_session.py](waiting_session.py), [remote_tool_ingress.py](remote_tool_ingress.py), [spawn_supervisor.py](spawn_supervisor.py) |
| Graph execution policy | [retry_timeout.py](retry_timeout.py) |

The graph examples still use supported APIs. They run with in-memory backends and
demonstrate waiting, spawn and retry semantics within one process. They do not
demonstrate shard runners or automatic process launch. Run them from the repository
root with `python -m examples.waiting_session` (substitute the example module).

`cli.py` and `coding_agent.py` are older graph-based chat loops: they persist chat
history while their execution graph stays in memory. Use `--model` to select a
model available through your configured provider; `--help` requires no model call.
`mailbox_cli.py` demonstrates process-local checkpoint ingestion. The Telegram
demo adds persistent event history and optional Redis mailboxes, but has not been
migrated to the atomic session checkpoint API. Neither is a shard-runner example.
