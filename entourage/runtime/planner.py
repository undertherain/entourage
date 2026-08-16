"""
Plan expansion — the one shared implementation of the graph algebra's
"plan → executions + edges" step, used by every runtime/backend combination.

A plan is a node reference (string name or callable), a policy-carrying
``Node`` wrapper, a ``Sequence``, a ``Parallel``, or a ``Conditional``
(see ``entourage.flow``).

Expansion is split into two phases so a returned plan can be committed
atomically with the node outcome that proposed it:

- ``stage_plan`` is pure — it walks the plan and produces a ``StagedPlan``
  (executions, edges, entry/exit points) without touching any store.
- ``apply_staged`` / ``expand_plan`` write a staged plan through the
  ``GraphStore`` primitives. ``expand_plan`` keeps the original one-call
  interface for session bootstrap and direct callers;
  ``GraphStore.commit_transition`` applies a staged plan together with the
  proposing node's completion as one commit.
"""

import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

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


@dataclass
class StagedPlan:
    """A plan expanded into concrete records, not yet written to any store.

    ``executions`` are dicts with ``exec_id``/``node_name``/``policy``;
    ``edges`` are ``(from_exec_id, to_exec_id, condition)`` triples internal
    to the plan. ``starts``/``end`` are the entry and exit points the caller
    wires into the surrounding graph.
    """

    executions: List[Dict[str, Any]] = field(default_factory=list)
    edges: List[Tuple[str, str, Optional[str]]] = field(default_factory=list)
    starts: List[str] = field(default_factory=list)
    end: str = ""


def stage_plan(
    plan: Plan,
    registry: Dict[str, Callable],
    existing_counts: Optional[Dict[str, int]] = None,
) -> StagedPlan:
    """
    Stage a plan into a ``StagedPlan`` without touching any store.

    ``existing_counts`` maps node names to how many executions of that name
    the session already holds; ``max_invocations`` policies are checked
    against it plus everything staged so far, matching the behavior of the
    former write-as-you-recurse expansion.
    """
    staged = StagedPlan()
    counts: Dict[str, int] = dict(existing_counts or {})

    def _add_execution(node_name: str, exec_id: str = None, policy=None) -> str:
        if exec_id is None:
            exec_id = uuid.uuid4().hex
        staged.executions.append(
            {"exec_id": exec_id, "node_name": node_name, "policy": policy or None}
        )
        counts[node_name] = counts.get(node_name, 0) + 1
        return exec_id

    def _stage(plan: Plan) -> Tuple[List[str], str]:
        if isinstance(plan, Sequence):
            segments = []  # list of (starts, end)
            for item in plan.items:
                starts, end = _stage(item)
                if segments:
                    prev_end = segments[-1][1]
                    for s in starts:
                        staged.edges.append((prev_end, s, None))
                segments.append((starts, end))
            return segments[0][0], segments[-1][1]

        elif isinstance(plan, Conditional):
            # A gate node checks the condition; edges out of it carry it too.
            gate_name = f"{GATE_PREFIX}{plan.condition}"
            gate_id = _add_execution(gate_name, exec_id=f"gate-{uuid.uuid4().hex[:8]}")
            inner_starts, inner_end = _stage(plan.plan)
            for s in inner_starts:
                staged.edges.append((gate_id, s, plan.condition))
            return [gate_id], inner_end

        elif isinstance(plan, Parallel):
            merge_id = _add_execution(MERGE, exec_id=f"merge-{uuid.uuid4().hex[:8]}")
            all_starts = []
            for item in plan.items:
                starts, end = _stage(item)
                all_starts.extend(starts)
                staged.edges.append((end, merge_id, None))
            return all_starts, merge_id

        elif isinstance(plan, Node):
            # Policy-carrying leaf: the policy is stored on the execution
            # itself, so every worker honors it regardless of who staged it.
            name = resolve_node(plan.node, registry)
            if plan.policy and plan.policy.get("max_invocations"):
                max_invocations = plan.policy["max_invocations"]
                if counts.get(name, 0) >= max_invocations:
                    raise RecursionLimitExceeded(
                        f"Node '{name}' exceeded max_invocations ({max_invocations})"
                    )
            exec_id = _add_execution(name, policy=plan.policy or None)
            return [exec_id], exec_id

        elif isinstance(plan, str) or callable(plan):
            name = resolve_node(plan, registry)
            exec_id = _add_execution(name)
            return [exec_id], exec_id

        else:
            raise ValueError(f"Unknown plan type: {type(plan)}")

    staged.starts, staged.end = _stage(plan)
    return staged


def apply_staged(store: GraphStore, session_id: str, staged: StagedPlan):
    """Write a staged plan through the store primitives (not atomic here)."""
    for ex in staged.executions:
        store.add_execution(
            session_id, ex["node_name"], exec_id=ex["exec_id"], policy=ex["policy"]
        )
    for from_id, to_id, condition in staged.edges:
        store.add_edge(session_id, from_id, to_id, condition)


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
    counts: Dict[str, int] = {}
    for ex in store.get_session_executions(session_id):
        counts[ex["node_name"]] = counts.get(ex["node_name"], 0) + 1
    staged = stage_plan(plan, registry, existing_counts=counts)
    apply_staged(store, session_id, staged)
    return staged.starts, staged.end
