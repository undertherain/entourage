"""
The Entourage runtime engine.

One engine, pluggable backends: a ``GraphStore`` for the execution graph and
a ``ReadyQueue`` for pointers to ready work — both constructor injections.
External services (Telegram, cron, etc.) push trigger messages to the queue;
the runtime polls it, advances the DAG in the store, and executes nodes.

Usage:
    runtime = QueueRuntime(
        node_registry={
            "triage_message": triage_fn,
            "generate_response": generate_fn,
            "send_message": send_fn,
        },
        store=SQLiteGraphStore(Path("data/entourage.db")),
        queue=InMemoryReadyQueue(),  # or SQSReadyQueue(...), Redis later
    )
    runtime.register_pipeline("telegram_reply", telegram_reply_pipeline)
    runtime.run()

Defaults preserve the original behavior (SQLite store + SQS queue) when
nothing is injected.
"""

import logging
import time
from pathlib import Path
from typing import Any, Callable, Dict, Union

from ..flow import Parallel, Sequence
from .interfaces import GraphStore, ReadyQueue
from .planner import END, GATE_PREFIX, HEAD, MERGE, Plan, expand_plan, resolve_node
from .store import DEFAULT_DB_PATH, SQLiteGraphStore

logger = logging.getLogger(__name__)


# Type for pipeline templates: callable that returns a plan (Sequence/Parallel/str)
PipelineTemplate = Callable[[Dict[str, Any]], Union[str, Sequence, Parallel]]


class QueueRuntime:
    def __init__(
        self,
        node_registry: Dict[str, Callable] = None,
        store: GraphStore = None,
        queue: ReadyQueue = None,
        queue_name: str = "entourage_tasks",
        region: str = "us-east-1",
        db_path: Path = DEFAULT_DB_PATH,
    ):
        self.node_registry = node_registry or {}
        self.pipelines: Dict[str, PipelineTemplate] = {}
        self.store = store if store is not None else SQLiteGraphStore(db_path)
        if queue is None:
            from .sqs import SQSReadyQueue

            queue = SQSReadyQueue(queue_name=queue_name, region=region)
        self.queue = queue

    # ── Registration ──────────────────────────────────────────

    def register_node(self, name: str, fn: Callable):
        """Register a named node callable."""
        self.node_registry[name] = fn

    def register_pipeline(self, trigger_name: str, template: PipelineTemplate):
        """
        Register a pipeline template for a trigger type.

        template receives the initial state and returns a plan
        (Sequence, Parallel, or a single node name string).
        """
        self.pipelines[trigger_name] = template

    # ── Session creation ──────────────────────────────────────

    def start_session(
        self, trigger, initial_state: Dict[str, Any] = None, plan: Plan = None
    ) -> str:
        """
        Create a new session from a trigger.

        `trigger` is normally a trigger name whose registered pipeline
        template produces the plan; passing a plan (Sequence/Parallel/
        callable) as `trigger` runs it directly as an ad-hoc session.
        An explicit `plan` overrides the pipeline lookup either way.
        """
        initial_state = initial_state or {}
        if plan is None and not isinstance(trigger, str):
            trigger, plan = "adhoc", trigger

        if plan is None:
            if trigger not in self.pipelines:
                raise ValueError(
                    f"No pipeline registered for trigger '{trigger}'. "
                    f"Available: {list(self.pipelines.keys())}"
                )
            plan = self.pipelines[trigger](initial_state)

        session_id = self.store.create_session(trigger, initial_state)

        # Create HEAD and END sentinels
        head_id = self.store.add_execution(session_id, HEAD, exec_id=f"head-{session_id[:8]}")
        end_id = self.store.add_execution(session_id, END, exec_id=f"end-{session_id[:8]}")

        plan_starts, plan_end = expand_plan(
            self.store, session_id, plan, self.node_registry
        )

        # Wire: HEAD → plan_starts, plan_end → END
        for s in plan_starts:
            self.store.add_edge(session_id, head_id, s)
        self.store.add_edge(session_id, plan_end, end_id)

        # Mark HEAD as completed immediately (it's just a sentinel)
        self.store.mark_completed(head_id, initial_state)

        self._enqueue_ready(session_id)

        logger.info("Started session %s for trigger '%s'", session_id, trigger)
        return session_id

    # ── Node execution ────────────────────────────────────────

    def _execute_node(self, exec_id: str, session_id: str):
        """Execute a single node and handle the result."""
        execution = self.store.get_execution(exec_id)
        if not execution:
            logger.error("Execution %s not found", exec_id)
            return

        # Idempotence guard: the queue is at-least-once (SQS redelivery,
        # startup recovery, double-ready on fan-in), so the same execution
        # can be delivered more than once. Only pending work runs.
        if execution["status"] != "pending":
            logger.debug(
                "Skipping execution %s (status=%s)", exec_id, execution["status"]
            )
            return

        node_name = execution["node_name"]

        # Handle END sentinel
        if node_name == END:
            self.store.mark_completed(exec_id, {})
            self.store.complete_session(session_id)
            logger.info("Session %s completed", session_id)
            return

        # Handle GATE — pass through state, conditions on edges do the filtering
        if node_name.startswith(GATE_PREFIX):
            input_state = self.store.collect_input_state(exec_id)
            self.store.mark_completed(exec_id, input_state)
            condition_key = node_name[len(GATE_PREFIX):]
            if not input_state.get(condition_key):
                # KNOWN GAP: skipping should resume after the gated sub-plan;
                # today it ends the whole session (fine for trailing gates).
                logger.info("Gate condition '%s' not met, skipping branch", condition_key)
                self.store.complete_session(session_id)
            else:
                self._enqueue_ready(session_id)
            return

        # Handle MERGE — just pass through merged state
        if node_name == MERGE:
            merged_state = self.store.collect_input_state(exec_id)
            self.store.mark_completed(exec_id, merged_state)
            self._enqueue_ready(session_id)
            return

        # Look up the callable
        if node_name not in self.node_registry:
            logger.error("Node '%s' not in registry", node_name)
            self.store.mark_failed(exec_id, f"Unknown node: {node_name}")
            return

        # Collect input state from parents
        input_state = self.store.collect_input_state(exec_id)
        self.store.mark_running(exec_id, input_state)

        try:
            fn = self.node_registry[node_name]
            result = fn(input_state)

            # Support two return styles:
            # 1. (new_state, plan) — Entourage style
            # 2. new_state — simple style (refs/workflow compat)
            if isinstance(result, tuple) and len(result) == 2:
                new_state, plan = result
            else:
                new_state, plan = result, None

            self.store.mark_completed(exec_id, new_state)

            # If the node returned a dynamic plan, inject it
            if plan is not None:
                plan_starts, plan_end = expand_plan(
                    self.store, session_id, plan, self.node_registry
                )
                self.store.rewire_after_plan_injection(
                    exec_id, plan_starts, plan_end, session_id
                )

            self._enqueue_ready(session_id)

        except Exception as e:
            logger.exception("Node '%s' failed: %s", node_name, e)
            self.store.mark_failed(exec_id, str(e))

    # ── Queue operations ──────────────────────────────────────

    def _enqueue_ready(self, session_id: str):
        """Find and enqueue all ready executions for a session."""
        ready = self.store.get_ready_executions(session_id)
        for ex in ready:
            self._send_to_queue(ex["id"], session_id)

    def _send_to_queue(self, exec_id: str, session_id: str):
        self.queue.send({
            "type": "execute",
            "exec_id": exec_id,
            "session_id": session_id,
            "time_created": time.time(),
        })
        logger.debug("Enqueued execution %s", exec_id)

    def send_trigger(self, trigger: str, state: Dict[str, Any]):
        """
        Send a trigger message to the queue.

        This is what external services call — e.g. Telegram listener.
        """
        self.queue.send({
            "type": "trigger",
            "trigger": trigger,
            "state": state,
            "time_created": time.time(),
        })
        logger.info("Sent trigger '%s' to queue", trigger)

    # ── Main loop ─────────────────────────────────────────────

    def _handle_message(self, body: Dict):
        msg_type = body.get("type")

        if msg_type == "trigger":
            self.start_session(body["trigger"], body.get("state", {}))

        elif msg_type == "execute":
            self._execute_node(body["exec_id"], body["session_id"])

        elif msg_type == "approval":
            # Future: resume a waiting node after user approval
            logger.info("Received approval for session %s", body.get("session_id"))

        else:
            logger.warning("Unknown message type: %s", msg_type)

    def run(self, poll_wait: float = 10, stop_when_idle: bool = False):
        """
        Main loop. Polls the queue and executes nodes.

        Also re-enqueues ready nodes of running sessions on startup
        (crash recovery). With stop_when_idle=True the loop exits on the
        first empty poll — correct for single-process in-memory runs,
        where an empty queue means no work can ever arrive.
        """
        logger.info("QueueRuntime starting...")

        # Crash recovery: check for any sessions that were running
        for session in self.store.get_running_sessions():
            logger.info("Recovering session %s", session["id"])
            self._enqueue_ready(session["id"])

        while True:
            messages = self.queue.receive(max_messages=10, wait_seconds=poll_wait)

            if not messages:
                if stop_when_idle:
                    logger.info("Queue idle, stopping.")
                    break
                continue

            self._process_messages(messages)

    def run_once(self, poll_wait: float = 1):
        """Process one batch of messages and return. Useful for testing."""
        messages = self.queue.receive(max_messages=10, wait_seconds=poll_wait)
        self._process_messages(messages)

    def _process_messages(self, messages):
        for message in messages:
            try:
                self._handle_message(message.payload)
                message.ack()
            except Exception as e:
                logger.exception("Error processing message: %s", e)
                message.nack()
