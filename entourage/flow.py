from typing import Any, Callable, Dict, List, Set, Tuple, Union

Node = Callable[
    [Union[Dict[str, Any], List[Dict[str, Any]]]], Tuple[Dict[str, Any], Any]
]  # Node: (state) -> (new_state, schedule)
Schedule = Any  # Will be Sequence, Parallel, or Node

# MEMO: add support for extensions like
# .with_timeout(10)
# .with_merge(my merge)


class ControlFlow:
    pass


class Conditional(ControlFlow):
    """Gate a sub-plan on a state key being truthy.

    Usage: Sequence("triage", Conditional("need_to_reply", Sequence("generate", "send")))
    If state["need_to_reply"] is falsy after triage, the inner plan is skipped.
    """

    def __init__(self, condition: str, plan: Union["Schedule", "Node"]):
        self.condition = condition
        self.plan = plan

    def __repr__(self):
        return f"Conditional({self.condition!r}, {self.plan})"


class Sequence(ControlFlow):
    """Represents a sequential chain of nodes or sub-schedules."""

    def __init__(self, *items: Union[Node, "Schedule"]):
        self.items = items

    def __repr__(self):
        return f"Sequence of {self.items}"


class Parallel(ControlFlow):
    """Represents a fork-join: execute items in parallel, join results."""

    def __init__(self, *items: Union[Node, "Schedule"]):
        self.items = items

    def __repr__(self):
        return f"Parallel execution of {self.items}"
