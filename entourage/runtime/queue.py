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
import threading
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Union

from ..flow import Parallel, Sequence
from .interfaces import GraphStore, ReadyQueue
from .planner import END, GATE_PREFIX, HEAD, MERGE, Plan, expand_plan, resolve_node
from .store import DEFAULT_DB_PATH, SQLiteGraphStore

logger = logging.getLogger(__name__)


# Type for pipeline templates: callable that returns a plan (Sequence/Parallel/str)
PipelineTemplate = Callable[[Dict[str, Any]], Union[str, Sequence, Parallel]]


class NodeTimeoutError(TimeoutError):
    """A node attempt exceeded its wall-clock timeout."""


def _call_with_timeout(fn: Callable, input_state: Dict, timeout: Optional[float]):
    """
    Call ``fn(input_state)``, raising NodeTimeoutError after ``timeout`` seconds.

    The call runs in a daemon thread so the engine can move on when it hangs.
    Python threads cannot be killed, so a timed-out call keeps running in the
    background until it returns on its own — its result is discarded. Real
    isolation (spawn process, terminate→kill) is the worker-hardening step
    (TODO B6); this gives the engine timeout *semantics* backends can rely on.
    """
    if not timeout:
        return fn(input_state)

    box: Dict[str, Any] = {}

    def target():
        try:
            box["result"] = fn(input_state)
        except BaseException as e:  # noqa: BLE001 — re-raised in the caller
            box["error"] = e

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        raise NodeTimeoutError(f"timed out after {timeout}s")
    if "error" in box:
        raise box["error"]
    return box["result"]


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
        self.node_policies: Dict[str, Dict[str, Any]] = {}
        self.pipelines: Dict[str, PipelineTemplate] = {}
        self.store = store if store is not None else SQLiteGraphStore(db_path)
        if queue is None:
            from .sqs import SQSReadyQueue

            queue = SQSReadyQueue(queue_name=queue_name, region=region)
        self.queue = queue

    # ── Registration ──────────────────────────────────────────

    def register_node(
        self,
        name: str,
        fn: Callable,
        max_attempts: int = None,
        timeout: float = None,
        retry_delay: float = None,
    ):
        """Register a named node callable, optionally with a default policy.

        The policy applies to every execution of this node; a per-leaf
        ``flow.Node(...)`` wrapper overrides it field by field. Defaults:
        max_attempts=1 (no retry), timeout=None (unbounded), retry_delay=0.
        """
        self.node_registry[name] = fn
        policy = {
            k: v
            for k, v in (
                ("max_attempts", max_attempts),
                ("timeout", timeout),
                ("retry_delay", retry_delay),
            )
            if v is not None
        }
        if policy:
            self.node_policies[name] = policy

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

        # retry_delay enforcement lives on the execution, not the message:
        # with at-least-once transports a duplicate delivery (startup
        # recovery, redelivery) would otherwise jump the delay.
        retry_at = execution.get("retry_at")
        if retry_at and time.time() < retry_at:
            time.sleep(min(retry_at - time.time(), 0.05))
            self._send_to_queue(exec_id, session_id, not_before=retry_at)
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
            self.store.fail_session(session_id)
            return

        # Effective policy: per-execution (flow.Node wrapper) overrides the
        # registered default field by field.
        policy = {
            **self.node_policies.get(node_name, {}),
            **(execution.get("policy") or {}),
        }
        attempt = execution.get("attempts", 0) + 1
        max_attempts = policy.get("max_attempts", 1)

        # Collect input state from parents
        input_state = self.store.collect_input_state(exec_id)
        self.store.mark_running(exec_id, input_state)

        try:
            fn = self.node_registry[node_name]
            result = _call_with_timeout(fn, input_state, policy.get("timeout"))

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
            if attempt < max_attempts:
                delay = policy.get("retry_delay", 0)
                logger.warning(
                    "Node '%s' failed on attempt %d/%d: %s — retrying%s",
                    node_name, attempt, max_attempts, e,
                    f" in {delay}s" if delay else "",
                )
                retry_at = time.time() + delay if delay else None
                self.store.mark_retrying(exec_id, str(e), retry_at=retry_at)
                self._send_to_queue(exec_id, session_id, not_before=retry_at)
            else:
                logger.exception(
                    "Node '%s' failed terminally after %d attempt(s): %s",
                    node_name, attempt, e,
                )
                self.store.mark_failed(exec_id, str(e))
                self.store.fail_session(session_id)

    # ── Queue operations ──────────────────────────────────────

    def _enqueue_ready(self, session_id: str):
        """Find and enqueue all ready executions for a session."""
        ready = self.store.get_ready_executions(session_id)
        for ex in ready:
            self._send_to_queue(ex["id"], session_id)

    def _send_to_queue(
        self, exec_id: str, session_id: str, not_before: float = None
    ):
        payload = {
            "type": "execute",
            "exec_id": exec_id,
            "session_id": session_id,
            "time_created": time.time(),
        }
        if not_before is not None:
            payload["not_before"] = not_before
        self.queue.send(payload)
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
            not_before = body.get("not_before")
            if not_before and time.time() < not_before:
                # Not due yet (retry_delay). Best-effort delay: nap briefly,
                # put it back, poll again. Durable timers are TODO C7.
                time.sleep(min(not_before - time.time(), 0.05))
                self.queue.send(body)
                return
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
