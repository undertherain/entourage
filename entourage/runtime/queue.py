"""
SQS-backed runtime for Entourage.

External services (Telegram, cron, etc.) push trigger messages to SQS.
This runtime polls SQS, manages DAG state in SQLite, and executes nodes.

Usage:
    runtime = QueueRuntime(node_registry={
        "triage_message": triage_fn,
        "generate_response": generate_fn,
        "send_message": send_fn,
    })
    runtime.register_pipeline("telegram_reply", telegram_reply_pipeline)
    runtime.run()
"""

import json
import logging
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

import boto3
from botocore.exceptions import ClientError

from ..flow import Conditional, ControlFlow, Parallel, Sequence
from .store import DEFAULT_DB_PATH, GraphStore

logger = logging.getLogger(__name__)

# Sentinel node names
HEAD = "__HEAD__"
END = "__END__"
MERGE = "__MERGE__"


# Type for pipeline templates: callable that returns a plan (Sequence/Parallel/str)
PipelineTemplate = Callable[[Dict[str, Any]], Union[str, Sequence, Parallel]]


class QueueRuntime:
    def __init__(
        self,
        node_registry: Dict[str, Callable] = None,
        queue_name: str = "entourage_tasks",
        region: str = "us-east-1",
        db_path: Path = DEFAULT_DB_PATH,
    ):
        self.node_registry = node_registry or {}
        self.pipelines: Dict[str, PipelineTemplate] = {}
        self.queue_name = queue_name
        self.region = region
        self.store = GraphStore(db_path)
        self.queue = self._get_or_create_queue()

    def _get_or_create_queue(self):
        sqs = boto3.resource("sqs", region_name=self.region)
        try:
            queue = sqs.get_queue_by_name(QueueName=self.queue_name)
            logger.info("Connected to queue %s", self.queue_name)
        except ClientError as e:
            if e.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue":
                queue = sqs.create_queue(QueueName=self.queue_name, Attributes={})
                logger.info("Created queue %s", self.queue_name)
            else:
                raise
        return queue

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

    # ── Plan expansion ────────────────────────────────────────

    def _expand_plan(
        self, plan: Union[str, Sequence, Parallel], session_id: str
    ) -> Tuple[List[str], str]:
        """
        Expand a plan into executions + edges in SQLite.
        Returns (start_exec_ids, end_exec_id).
        """
        if isinstance(plan, str):
            # Single named node
            exec_id = self.store.add_execution(session_id, plan)
            return [exec_id], exec_id

        elif isinstance(plan, Sequence):
            all_segments = []  # list of (starts, end)
            for item in plan.items:
                starts, end = self._expand_plan(item, session_id)
                if all_segments:
                    # Wire previous end → current starts
                    prev_end = all_segments[-1][1]
                    for s in starts:
                        self.store.add_edge(session_id, prev_end, s)
                all_segments.append((starts, end))
            return all_segments[0][0], all_segments[-1][1]

        elif isinstance(plan, Conditional):
            # Create a gate node that checks the condition
            gate_name = f"__GATE__{plan.condition}"
            gate_id = self.store.add_execution(
                session_id, gate_name, exec_id=f"gate-{uuid.uuid4().hex[:8]}"
            )
            # Expand the inner plan
            inner_starts, inner_end = self._expand_plan(plan.plan, session_id)
            # Wire: gate → inner_starts (with condition on edges)
            for s in inner_starts:
                self.store.add_edge(session_id, gate_id, s, condition=plan.condition)
            return [gate_id], inner_end

        elif isinstance(plan, Parallel):
            # Create a merge node
            merge_id = self.store.add_execution(
                session_id, MERGE, exec_id=f"merge-{uuid.uuid4().hex[:8]}"
            )
            all_starts = []
            for item in plan.items:
                starts, end = self._expand_plan(item, session_id)
                all_starts.extend(starts)
                # Wire each branch end → merge
                self.store.add_edge(session_id, end, merge_id)
            return all_starts, merge_id

        else:
            raise ValueError(f"Unknown plan type: {type(plan)}")

    # ── Session creation ──────────────────────────────────────

    def start_session(
        self, trigger: str, initial_state: Dict[str, Any], plan=None
    ) -> str:
        """
        Create a new session from a trigger.

        If plan is provided, use it directly.
        Otherwise, look up registered pipeline template for the trigger.
        """
        if plan is None:
            if trigger not in self.pipelines:
                raise ValueError(
                    f"No pipeline registered for trigger '{trigger}'. "
                    f"Available: {list(self.pipelines.keys())}"
                )
            plan = self.pipelines[trigger](initial_state)

        session_id = self.store.create_session(trigger, initial_state)

        # Create HEAD and END
        head_id = self.store.add_execution(session_id, HEAD, exec_id=f"head-{session_id[:8]}")
        end_id = self.store.add_execution(session_id, END, exec_id=f"end-{session_id[:8]}")

        # Expand plan
        plan_starts, plan_end = self._expand_plan(plan, session_id)

        # Wire: HEAD → plan_starts, plan_end → END
        for s in plan_starts:
            self.store.add_edge(session_id, head_id, s)
        self.store.add_edge(session_id, plan_end, end_id)

        # Mark HEAD as completed immediately (it's just a sentinel)
        self.store.mark_completed(head_id, initial_state)

        # Enqueue ready nodes
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

        node_name = execution["node_name"]

        # Handle END sentinel
        if node_name == END:
            self.store.mark_completed(exec_id, {})
            self.store.complete_session(session_id)
            logger.info("Session %s completed", session_id)
            return

        # Handle GATE — pass through state, conditions on edges do the filtering
        if node_name.startswith("__GATE__"):
            input_state = self.store.collect_input_state(exec_id)
            self.store.mark_completed(exec_id, input_state)
            # Check if condition is met — if not, skip children and go to END
            condition_key = node_name[len("__GATE__"):]
            if not input_state.get(condition_key):
                logger.info("Gate condition '%s' not met, skipping branch", condition_key)
                # Find the session's END node and mark session complete
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
                plan_starts, plan_end = self._expand_plan(plan, session_id)
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
        self.queue.send_message(
            MessageBody=json.dumps({
                "type": "execute",
                "exec_id": exec_id,
                "session_id": session_id,
                "time_created": time.time(),
            })
        )
        logger.debug("Enqueued execution %s", exec_id)

    def send_trigger(self, trigger: str, state: Dict[str, Any]):
        """
        Send a trigger message to the queue.

        This is what external services call — e.g. Telegram listener.
        """
        self.queue.send_message(
            MessageBody=json.dumps({
                "type": "trigger",
                "trigger": trigger,
                "state": state,
                "time_created": time.time(),
            })
        )
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

    def run(self, poll_wait: int = 10):
        """
        Main loop. Polls SQS and executes nodes.

        Also checks for any ready nodes from running sessions on startup
        (crash recovery).
        """
        logger.info("QueueRuntime starting, polling %s...", self.queue_name)

        # Crash recovery: check for any sessions that were running
        for session in self.store.get_running_sessions():
            logger.info("Recovering session %s", session["id"])
            self._enqueue_ready(session["id"])

        while True:
            messages = self.queue.receive_messages(
                MaxNumberOfMessages=10,
                WaitTimeSeconds=poll_wait,
            )

            if not messages:
                continue

            for message in messages:
                try:
                    body = json.loads(message.body)
                    self._handle_message(body)
                    message.delete()
                except Exception as e:
                    logger.exception("Error processing message: %s", e)
                    # Message will return to queue after visibility timeout

    def run_once(self):
        """Process one batch of messages and return. Useful for testing."""
        messages = self.queue.receive_messages(
            MaxNumberOfMessages=10,
            WaitTimeSeconds=1,
        )
        for message in messages:
            try:
                body = json.loads(message.body)
                self._handle_message(body)
                message.delete()
            except Exception as e:
                logger.exception("Error processing message: %s", e)
