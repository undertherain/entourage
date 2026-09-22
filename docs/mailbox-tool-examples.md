# Tool calls that wake their caller

Start with the smaller [resident and resumable agents](../examples/mailboxes/README.md).
This page retains the longer explicit request/reply and clarification walkthrough.

The coarse-grained unit is a durable agent session. An activation receives its
saved state and mail, proposes state and outgoing messages, then releases compute.
The agent does not need a graph node for each reasoning or tool step.

[`examples/mailboxes/request_reply.py`](../examples/mailboxes/request_reply.py) contains two examples
using the current `LocalSessions` core. Every command below runs in a fresh
process. `tick` is a tiny runtime driver: claim one ready activation, dispatch its
handler, commit its proposal, exit. A running scheduler would repeat that work;
mail by itself does not launch a process in this slice.

## 1. Call a tool, exit, wake on its result

```bash
python examples/mailboxes/request_reply.py /tmp/lookup.db start
python examples/mailboxes/request_reply.py /tmp/lookup.db tick  # agent publishes request, parks
python examples/mailboxes/request_reply.py /tmp/lookup.db tick  # tool publishes mock weather, parks
python examples/mailboxes/request_reply.py /tmp/lookup.db tick  # same agent wakes with result
python examples/mailboxes/request_reply.py /tmp/lookup.db tick  # no work
python examples/mailboxes/request_reply.py /tmp/lookup.db show
```

The request carries `reply_to`, the caller's mailbox address, and `exchange`, the
identity of this tool call. The tool returns correlated mail to that address.
Saving the agent's waiting state and publishing the request happen in one commit;
saving the tool's state and publishing its result also happen in one commit.
There is no live call stack waiting for a return value.

## 2. The tool needs a user answer

```bash
python examples/mailboxes/request_reply.py /tmp/tour.db start --scenario clarify
python examples/mailboxes/request_reply.py /tmp/tour.db tick  # agent requests tour
python examples/mailboxes/request_reply.py /tmp/tour.db tick  # tool asks morning or afternoon
python examples/mailboxes/request_reply.py /tmp/tour.db tick  # agent forwards question to UI
python examples/mailboxes/request_reply.py /tmp/tour.db tick  # both parked; no work
python examples/mailboxes/request_reply.py /tmp/tour.db show  # durable question is visible
python examples/mailboxes/request_reply.py /tmp/tour.db answer "Afternoon"
python examples/mailboxes/request_reply.py /tmp/tour.db tick  # agent wakes, forwards answer
python examples/mailboxes/request_reply.py /tmp/tour.db tick  # tool wakes, returns confirmation
python examples/mailboxes/request_reply.py /tmp/tour.db tick  # agent wakes, records result for UI
python examples/mailboxes/request_reply.py /tmp/tour.db show
```

Parking the parent leaves the tool session intact. The outstanding tool call
does not prevent the parent from receiving other mail: here it wakes to handle
the user's answer while the tool is still pending. The UI question and final
result are committed publications, so an exit between commit and console output
does not lose them. `show` reads them without consuming an agent's input lease.

Use fresh database paths for a new run, or `start --trip tokyo` to add a second
trip to the same database (include `--scenario clarify` for the tour variant).
`answer --trip tokyo "Morning"` addresses only that trip; `tick` schedules across
trips. Both trips share handler code but have separate state and mailboxes.

## Scope of the example

The `agent` and `tool` functions return proposals; the driver retains the lease
and calls `commit`. This is a teaching convention over the trusted in-process
API, not the future validated executable protocol. Agents remain parked after
their result rather than terminally closing their mailbox.

All tools are deterministic mocks. No booking, remote API, model, or external
side effect occurs. Stable operation IDs make local publication replay-safe;
they do not make an external API call exactly-once. The reply address is routing,
not an authorization capability. This demo supports one tool exchange and one
user answer per trip; it is not an amendment or multi-call conversation policy.

Addresses are explicitly pre-created, without atomic bootstrap or child spawn.
The handler table lives in the example. For implemented registration, correlated
reply helpers, bounded batches and basic claim rotation, see the
[registered agent](../examples/mailboxes/registered/README.md). Runtime-launched
subprocesses and automatic child creation remain [next steps](../NOW.md).
The UI is a durable inspection mailbox, not a transport adapter; repeated `show`
calls intentionally display the same messages.
