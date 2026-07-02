"""
Local in-memory runtime — the same engine as QueueRuntime, on the trivial
backends (InMemoryGraphStore + InMemoryReadyQueue).

Replaces the old divergent `runtime.core.Runtime`: nodes can be plain
callables (functions or callable instances) passed directly in plans; they
are auto-registered by name. `run()` returns once all sessions complete.
"""

import logging
import os
from typing import Any, Dict

from .interfaces import GraphStore
from .memory import InMemoryGraphStore, InMemoryReadyQueue
from .planner import Plan
from .queue import QueueRuntime

logger = logging.getLogger(__name__)


class Runtime(QueueRuntime):
    def __init__(self, debug: bool = False, store: GraphStore = None):
        super().__init__(
            node_registry={},
            store=store if store is not None else InMemoryGraphStore(),
            queue=InMemoryReadyQueue(),
        )
        self.debug = debug

    def start_session(
        self, plan: Plan, initial_state: Dict[str, Any] = None, **kwargs
    ) -> str:
        """Start a session directly from a plan (callable, Sequence, Parallel)."""
        session_id = super().start_session(plan, initial_state, **kwargs)
        if self.debug:
            logger.info("Started session %s", session_id)
        return session_id

    def run(self, poll_wait: float = 0.05, stop_when_idle: bool = True):
        """Process until all sessions complete (single-process, so an empty
        queue means nothing more can arrive)."""
        super().run(poll_wait=poll_wait, stop_when_idle=stop_when_idle)

    def visualize_graph(self, session_id: str, out_dir: str = "logs"):
        """Render the session's execution graph with graphviz (optional dep)."""
        from graphviz import Digraph

        graph = self.store.get_session_graph(session_id)
        dot = Digraph(comment=f"Execution Graph - Session {session_id[:8]}")
        for ex in graph["executions"]:
            dot.node(ex["id"], f"{ex['node_name']}\n({ex['id'][:8]}, {ex['status']})")
        for edge in graph["edges"]:
            label = edge.get("condition") or ""
            dot.edge(edge["from_exec_id"], edge["to_exec_id"], label=label)

        os.makedirs(out_dir, exist_ok=True)
        filename = os.path.join(out_dir, f"execution_graph_{session_id[:8]}")
        dot.render(filename, view=False)
        return filename
