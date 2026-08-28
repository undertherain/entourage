from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

NodeFn = Callable[
    [Union[Dict[str, Any], List[Dict[str, Any]]]], Tuple[Dict[str, Any], Any]
]  # NodeFn: (state) -> (new_state, schedule)
Schedule = Any  # Will be Sequence, Parallel, or Node

# MEMO: add support for extensions like
# .with_merge(my merge)


class ControlFlow:
    pass

class RecursionLimitExceeded(Exception):
    """Raised when a node exceeds its max_invocations limit."""
    pass


class Node(ControlFlow):
    """A plan leaf (node name or callable) with an execution policy.

    Wrap a node wherever a plain name/callable would go to attach retry and
    timeout controls:

        Sequence("fetch", Node("call_api", max_attempts=3, timeout=10), "report")

    - ``max_attempts``: total tries before the execution (and its session) is
      marked failed. Default 1 = no retry.
    - ``timeout``: wall-clock seconds per attempt; a timed-out attempt counts
      as a failure and is retried like any other.
    - ``retry_delay``: seconds to wait before re-enqueueing a failed attempt.

    Only the fields you set here are stored on the execution; unset fields
    fall back to the policy registered with ``register_node`` (if any), then
    to the defaults above. The policy travels with the execution in the
    GraphStore, so in multi-worker setups every worker honors it regardless
    of which worker expanded the plan.
    """

    def __init__(
        self,
        node: Union[str, NodeFn],
        max_attempts: Optional[int] = None,
        timeout: Optional[float] = None,
        retry_delay: Optional[float] = None,
        max_invocations: Optional[int] = None,
    ):
        if max_attempts is not None and max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if max_invocations is not None and max_invocations < 1:
            raise ValueError("max_invocations must be >= 1")
        self.node = node
        self.policy = {
            k: v
            for k, v in (
                ("max_attempts", max_attempts),
                ("timeout", timeout),
                ("retry_delay", retry_delay),
                ("max_invocations", max_invocations),
            )
            if v is not None
        }

    def __repr__(self):
        return f"Node({self.node!r}, {self.policy})"


class WaitForMailbox(ControlFlow):
    """Park the session until mailbox events arrive (or a timeout fires).

    A plan leaf: wherever a node could go, a wait can go. The execution
    parks durably — status ``waiting``, holding no worker — and wakes when
    its conversation has claimable events or ``timeout`` seconds elapsed
    since it first parked. It completes with the drained events merged into
    state under ``"events"`` (each a mailbox event dict); on timeout the
    single delivered event is ``{"kind": "system", "source": "timer", ...}``
    so the successor node re-decides instead of hanging forever.

    Drained events are acknowledged inside the same transition commit that
    completes the wait — the commit, not the claim lease, is the proof of
    incorporation.

    - ``conversation``: mailbox conversation to wait on. Default: the
      ``conversation_id`` key of the input state.
    - ``timeout``: seconds before waking with a timer event. Default: wait
      indefinitely (supervision loops should normally set one).
    - ``limit``: maximum events drained per wake.
    """

    def __init__(
        self,
        conversation: Optional[str] = None,
        timeout: Optional[float] = None,
        limit: int = 20,
    ):
        if timeout is not None and timeout < 0:
            raise ValueError("timeout must be >= 0")
        if limit < 1:
            raise ValueError("limit must be >= 1")
        self.params = {
            k: v
            for k, v in (
                ("conversation", conversation),
                ("timeout", timeout),
                ("limit", limit),
            )
            if v is not None
        }

    def __repr__(self):
        return f"WaitForMailbox({self.params})"


class Conditional(ControlFlow):
    """Gate a sub-plan on a state key being truthy.

    Usage: Sequence("triage", Conditional("need_to_reply", Sequence("generate", "send")))
    If state["need_to_reply"] is falsy after triage, the inner plan is skipped.
    """

    def __init__(self, condition: str, plan: Union["Schedule", NodeFn]):
        self.condition = condition
        self.plan = plan

    def __repr__(self):
        return f"Conditional({self.condition!r}, {self.plan})"


class Sequence(ControlFlow):
    """Represents a sequential chain of nodes or sub-schedules."""

    def __init__(self, *items: Union[NodeFn, "Schedule"]):
        self.items = items

    def __repr__(self):
        return f"Sequence of {self.items}"


class Parallel(ControlFlow):
    """Represents a fork-join: execute items in parallel, join results."""

    def __init__(self, *items: Union[NodeFn, "Schedule"]):
        self.items = items

    def __repr__(self):
        return f"Parallel execution of {self.items}"
