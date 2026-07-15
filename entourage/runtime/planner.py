"""
Plan expansion — the one shared implementation of the graph algebra's
"plan → executions + edges" step, used by every runtime/backend combination.

A plan is a node reference (string name or callable), a policy-carrying
``Node`` wrapper, a ``Sequence``, a ``Parallel``, or a ``Conditional``
(see ``entourage.flow``). Expansion writes
executions and edges into a ``GraphStore`` and returns the plan's entry and
exit points so the caller can wire it into the surrounding graph.
"""

import uuid
from typing import Any, Callable, Dict, List, Tuple, Union

from ..flow import Conditional, Node, Parallel, Sequence, RecursionLimitExceeded
from .interfaces import GraphStore

# Sentinel node names
HEAD = "__HEAD__"
END = "__END__"
MERGE = "__MERGE__"
GATE_PREFIX = "__GATE__"

Plan = Union[str, Callable, Node, Sequence, Parallel, Conditional]


def resolve_node(item: Union[str, Callable], registry: Dict[str, Callable]) -> str:
    """
    Resolve a plan leaf to a registry name.

    Strings pass through untouched (assumed pre-registered). Callables are
    auto-registered under their ``__name__`` (or class name for instances);
    re-resolving the same object is idempotent, a different object colliding
    on name gets a disambiguated one.
    """
    if isinstance(item, str):
        return item
    if callable(item):
        name = getattr(item, "__name__", None) or type(item).__name__
        current = registry.get(name)
        if current is None:
            registry[name] = item
        elif current is not item:
            name = f"{name}_{id(item):x}"
            registry[name] = item
        return name
    raise TypeError(f"Plan leaf must be a node name or callable, got {type(item)}")


def expand_plan(
    store: GraphStore,
    session_id: str,
    plan: Plan,
    registry: Dict[str, Callable],
) -> Tuple[List[str], str]:
    """
    Expand a plan into executions + edges in the store.
    Returns (start_exec_ids, end_exec_id).
    """
    if isinstance(plan, Sequence):
        segments = []  # list of (starts, end)
        for item in plan.items:
            starts, end = expand_plan(store, session_id, item, registry)
            if segments:
                prev_end = segments[-1][1]
                for s in starts:
                    store.add_edge(session_id, prev_end, s)
            segments.append((starts, end))
        return segments[0][0], segments[-1][1]

    elif isinstance(plan, Conditional):
        # A gate node checks the condition; edges out of it carry it too.
        gate_name = f"{GATE_PREFIX}{plan.condition}"
        gate_id = store.add_execution(
            session_id, gate_name, exec_id=f"gate-{uuid.uuid4().hex[:8]}"
        )
        inner_starts, inner_end = expand_plan(store, session_id, plan.plan, registry)
        for s in inner_starts:
            store.add_edge(session_id, gate_id, s, condition=plan.condition)
        return [gate_id], inner_end

    elif isinstance(plan, Parallel):
        merge_id = store.add_execution(
            session_id, MERGE, exec_id=f"merge-{uuid.uuid4().hex[:8]}"
        )
        all_starts = []
        for item in plan.items:
            starts, end = expand_plan(store, session_id, item, registry)
            all_starts.extend(starts)
            store.add_edge(session_id, end, merge_id)
        return all_starts, merge_id

    elif isinstance(plan, Node):
        # Policy-carrying leaf: the policy is stored on the execution itself,
        # so every worker honors it regardless of who expanded the plan.
        name = resolve_node(plan.node, registry)
        
        if plan.policy and plan.policy.get("max_invocations"):
            max_invocations = plan.policy["max_invocations"]
            executions = store.get_session_executions(session_id)
            count = sum(1 for ex in executions if ex["node_name"] == name)
            if count >= max_invocations:
                raise RecursionLimitExceeded(f"Node '{name}' exceeded max_invocations ({max_invocations})")

        exec_id = store.add_execution(session_id, name, policy=plan.policy or None)
        return [exec_id], exec_id

    elif isinstance(plan, str) or callable(plan):
        name = resolve_node(plan, registry)
        exec_id = store.add_execution(session_id, name)
        return [exec_id], exec_id

    else:
        raise ValueError(f"Unknown plan type: {type(plan)}")
