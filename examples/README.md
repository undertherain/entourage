# Examples

For the current coarse-grained agent direction, start in
**[mailboxes/](mailboxes/README.md)**: a resident mailbox loop and a minimal
save/exit/resume agent, with plumbing kept in a shared helper file.
The [registered agent](mailboxes/registered/README.md) uses the runtime API with
an adjacent manifest and handles user amendments across a dispatcher restart.

The existing examples cover these other surfaces:

| Area | Examples |
| --- | --- |
| Interactive agents and transports | [cli.py](cli.py), [coding_agent.py](coding_agent.py), [mailbox_cli.py](mailbox_cli.py), [telegram_group_manager.py](telegram_group_manager.py) |
| Graph waiting and coordination | [waiting_session.py](waiting_session.py), [remote_tool_ingress.py](remote_tool_ingress.py), [spawn_supervisor.py](spawn_supervisor.py) |
| Graph execution policy | [retry_timeout.py](retry_timeout.py) |
