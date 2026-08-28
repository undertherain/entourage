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

from ..flow import Parallel, Sequence, RecursionLimitExceeded
from ..transition import normalize_result
from .interfaces import GraphStore, ReadyQueue
from .planner import END, GATE_PREFIX, HEAD, MERGE, WAIT, Plan, expand_plan, stage_plan
from .store import DEFAULT_DB_PATH, SQLiteGraphStore
from .client import TriggerClient
from .gc import RetentionPolicy, collect_terminal_sessions

logger = logging.getLogger(__name__)

MAX_SESSION_NODES = 1000


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
        retention_policy: RetentionPolicy = RetentionPolicy(),
        mailbox=None,
        mailbox_resolver: Callable[[str], Any] = None,
        monitors=None,
    ):
        self.node_registry = node_registry or {}
        self.node_policies: Dict[str, Dict[str, Any]] = {}
        self.pipelines: Dict[str, PipelineTemplate] = {}
        # Maps opaque publication targets to Mailbox instances. Addresses
        # stay stable names behind this injectable seam — never hostnames
        # or backend details. Default: only "self" resolves, to `mailbox`.
        self.mailbox_resolver = mailbox_resolver
        self.store = store if store is not None else SQLiteGraphStore(db_path)
        if queue is None:
            from .sqs import SQSReadyQueue

            queue = SQSReadyQueue(queue_name=queue_name, region=region)
        self.queue = queue
        self.retention_policy = retention_policy
        self.mailbox = mailbox
        self.monitors = monitors
        self._last_gc_at = 0.0

    @classmethod
    def from_config(cls, config, **kwargs):
        """Construct graph, queue, mailbox, and monitors from one backend profile."""
        resources = config.resources()
        return cls(
            store=resources.graph_store,
            queue=resources.ready_queue,
            mailbox=resources.mailbox,
            monitors=getattr(resources, "monitors", None),
            **kwargs,
        )

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
        self,
        trigger,
        initial_state: Dict[str, Any] = None,
        plan: Plan = None,
        serial_key: str = None,
    ) -> Optional[str]:
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

        session_id = self.store.create_session(
            trigger, initial_state, serial_key=serial_key
        )
        if session_id is None:
            return None

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

        # Handle WAIT — drain now, or park durably until wake
        if node_name == WAIT:
            self._execute_wait(execution, exec_id, session_id)
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

            # The node's return is a proposed transition: result state, an
            # optional plan splice with its rewiring, and mailbox effects.
            # Reads, staging, and effect validation happen first; then the
            # store commits the whole transition as one unit, so a crash
            # leaves either the complete transition or a still-running
            # execution recovery will re-run — never a completed node whose
            # returned plan or effects were lost.
            transition = normalize_result(result)
            staged = children = None
            if transition.plan is not None:
                executions = self.store.get_session_executions(session_id)
                if len(executions) >= MAX_SESSION_NODES:
                    raise RecursionLimitExceeded(
                        f"Session {session_id} exceeded global hard limit of {MAX_SESSION_NODES} executions"
                    )
                counts: Dict[str, int] = {}
                for ex in executions:
                    counts[ex["node_name"]] = counts.get(ex["node_name"], 0) + 1
                staged = stage_plan(
                    transition.plan, self.node_registry, existing_counts=counts
                )
                children = self.store.get_children(exec_id)
            effects = self._stage_effects(exec_id, transition)

            self.store.commit_transition(
                exec_id, session_id, transition.state, staged, children, effects
            )

            if effects is not None:
                self._apply_effects_guarded(exec_id, effects)

            self._enqueue_ready(session_id)

        except RecursionLimitExceeded as e:
            logger.exception("Recursion limit exceeded for node '%s': %s", node_name, e)
            self.store.mark_failed(exec_id, str(e))
            self.store.fail_session(session_id)

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

    # ── Waiting sessions ──────────────────────────────────────

    def _execute_wait(self, execution: Dict, exec_id: str, session_id: str):
        """Run a ``__WAIT__`` execution: drain, time out, or park.

        The wait is a pure function of (mailbox state, clock), so every
        path is safe to re-run: a spurious wake re-parks, a crash between
        claim and commit lets the lease expire and re-claims, and the
        timeout anchor persists in the execution's ``wake`` condition.

        - Claimable events → complete with them merged into state under
          ``"events"``; their acknowledgement rides the transition commit.
        - No events, deadline passed → complete with a single
          ``kind: system`` timer event, so the successor re-decides.
        - Otherwise → park: status ``waiting`` plus the wake condition.
        """
        input_state = self.store.collect_input_state(exec_id)
        wait = (execution.get("policy") or {}).get("wait", {})
        conversation = wait.get("conversation") or input_state.get("conversation_id")
        if not conversation or self.mailbox is None:
            reason = (
                "WaitForMailbox needs a conversation (parameter or "
                "'conversation_id' in state)"
                if not conversation
                else "WaitForMailbox needs a mailbox configured on the runtime"
            )
            logger.error("%s (execution %s)", reason, exec_id)
            self.store.mark_failed(exec_id, reason)
            self.store.fail_session(session_id)
            return

        events = self.mailbox.claim(
            conversation,
            consumer=f"wait:{exec_id}",
            limit=wait.get("limit", 20),
        )
        if events:
            effects = {
                "acknowledge": [{
                    "conversation_id": conversation,
                    "event_ids": [e["event_id"] for e in events],
                }]
            }
            result = {**input_state, "events": events}
            self.store.commit_transition(
                exec_id, session_id, result, None, None, effects
            )
            self._apply_effects_guarded(exec_id, effects)
            self._enqueue_ready(session_id)
            return

        # The timeout anchors at the first park and survives re-parking.
        timeout = wait.get("timeout")
        previous = execution.get("wake") or {}
        wake_at = previous.get("wake_at")
        if wake_at is None and timeout is not None:
            wake_at = time.time() + timeout

        if wake_at is not None and time.time() >= wake_at:
            timer_event = {
                "kind": "system",
                "source": "timer",
                "payload": {"timeout": timeout},
            }
            result = {**input_state, "events": [timer_event]}
            self.store.commit_transition(exec_id, session_id, result)
            self._enqueue_ready(session_id)
            return

        wake = {"conversation": conversation}
        if wake_at is not None:
            wake["wake_at"] = wake_at
        self.store.mark_waiting(exec_id, wake)
        logger.debug(
            "Parked execution %s on conversation %s%s",
            exec_id, conversation,
            f" until {wake_at:.0f}" if wake_at else "",
        )

    def wake_due_waits(self) -> int:
        """Wake parked executions whose mail arrived or deadline passed.

        The transport-neutral wake baseline: called every engine loop
        iteration and after applying publications, and callable by ingress
        adapters after an external append. Waking is idempotent (only a
        waiting execution flips to pending), so tick and append racing is
        harmless. Returns the number of executions woken.
        """
        woken = 0
        now = time.time()
        for ex in self.store.get_waiting_executions():
            wake = ex.get("wake") or {}
            due = wake.get("wake_at") is not None and now >= wake["wake_at"]
            has_mail = (
                self.mailbox is not None
                and wake.get("conversation")
                and self.mailbox.claimable_count(wake["conversation"]) > 0
            )
            if not (due or has_mail):
                continue
            if self.store.wake_execution(ex["id"]):
                self._send_to_queue(ex["id"], ex["session_id"])
                woken += 1
        return woken

    def tick_monitors(self) -> int:
        """Fire lapsed expectations as ``kind: system`` mail.

        Lazy evaluation at fire time — nothing was cancelled on arrival;
        satisfied deadline monitors are already gone and refreshed
        heartbeats are not due. The lapse event id is derived from the
        monitor id (and heartbeat cycle), so a crash between append and
        ``mark_lapsed`` replays into the mailbox dedupe, not a duplicate.
        Returns the number of lapses fired.
        """
        if self.monitors is None or self.mailbox is None:
            return 0
        fired = 0
        now = time.time()
        for record in self.monitors.due(now):
            event = {
                "event_id": f"lapse-{record['monitor_id']}-{record.get('cycles', 0)}",
                "kind": "system",
                "source": "monitor",
                "payload": {
                    key: record[key]
                    for key in (
                        "monitor_id", "correlation_id", "source",
                        "deadline", "interval",
                    )
                    if record.get(key) is not None
                },
            }
            event["payload"]["reason"] = (
                "deadline" if record.get("deadline") is not None else "heartbeat"
            )
            self.mailbox.append(record["notify"], event)
            self.monitors.mark_lapsed(record["monitor_id"], now)
            logger.warning(
                "Monitor %s lapsed (%s); notified %s",
                record["monitor_id"], event["payload"]["reason"], record["notify"],
            )
            fired += 1
        return fired

    def observe_monitors(self, correlation_id: str = None, source: str = None) -> int:
        """Feed an external observation (ingress adapters call this)."""
        if self.monitors is None:
            return 0
        return self.monitors.observe(correlation_id=correlation_id, source=source)

    def waiting_conversations(self) -> set:
        """Conversations some parked execution is currently waiting on.

        Ingress routing uses this to decide whether a correlated result
        still has a waiter — a late result whose wait already timed out
        falls through to the resident inbox instead."""
        return {
            (ex.get("wake") or {}).get("conversation")
            for ex in self.store.get_waiting_executions()
        } - {None}

    def _next_wake_at(self) -> Optional[float]:
        deadlines = [
            (ex.get("wake") or {}).get("wake_at")
            for ex in self.store.get_waiting_executions()
        ]
        deadlines = [d for d in deadlines if d is not None]
        return min(deadlines) if deadlines else None

    # ── Mailbox effects (transactional outbox) ────────────────

    def _resolve_mailbox(self, target: str):
        if self.mailbox_resolver is not None:
            return self.mailbox_resolver(target)
        if target == "self" and self.mailbox is not None:
            return self.mailbox
        raise KeyError(f"no mailbox resolves for publication target {target!r}")

    def _stage_effects(self, exec_id: str, transition) -> Optional[Dict[str, Any]]:
        """Serialize a transition's mailbox effects for the commit.

        Validation happens here, before anything is committed: an
        unresolvable publication target or an acknowledgement without a
        configured mailbox fails the node through the normal retry/fail
        path instead of leaving a committed transition with undeliverable
        effects. Publication idempotency keys are derived deterministically
        from the committing execution, so a replay cannot double-deliver.
        """
        if not transition.has_effects:
            return None
        if transition.acknowledge and self.mailbox is None:
            raise ValueError(
                "transition acknowledges mailbox events, but the runtime "
                "has no mailbox configured"
            )
        effects: Dict[str, Any] = {}
        if transition.acknowledge:
            effects["acknowledge"] = [
                {
                    "conversation_id": ack.conversation_id,
                    "event_ids": list(ack.event_ids),
                }
                for ack in transition.acknowledge
            ]
        if transition.publish:
            publications = []
            for index, pub in enumerate(transition.publish):
                self._resolve_mailbox(pub.target)
                publications.append({
                    "target": pub.target,
                    "conversation": pub.conversation,
                    "event": pub.event,
                    "event_id": pub.event_id or f"txn-{exec_id}-{index}",
                })
            effects["publish"] = publications
        if transition.arm or transition.disarm:
            if self.monitors is None:
                raise ValueError(
                    "transition arms/disarms monitors, but the runtime has "
                    "no monitor store configured"
                )
        if transition.arm:
            from ..monitors import monitor_to_dict

            armed = []
            for index, monitor in enumerate(transition.arm):
                record = monitor_to_dict(monitor)
                # Deterministic default id: replay after a crash re-arms
                # the same monitor instead of twinning it.
                record.setdefault("monitor_id", f"mon-{exec_id}-{index}")
                armed.append(record)
            effects["arm"] = armed
        if transition.disarm:
            effects["disarm"] = list(transition.disarm)
        return effects

    def _apply_effects(self, exec_id: str, effects: Dict[str, Any]):
        """Apply a committed transition's mailbox effects, then clear them.

        Idempotent by construction — acknowledgements use force semantics
        (the commit, not the lease, is the proof of incorporation) and
        publications append with fixed event ids the mailboxes dedupe on —
        so a crash anywhere in here is repaired by replaying at recovery.
        """
        for ack in effects.get("acknowledge", []):
            self.mailbox.acknowledge(
                ack["conversation_id"],
                consumer="runtime",
                event_ids=ack["event_ids"],
                force=True,
            )
        for pub in effects.get("publish", []):
            event = dict(pub["event"])
            event["event_id"] = pub["event_id"]
            self._resolve_mailbox(pub["target"]).append(pub["conversation"], event)
            # Publications feed monitors: a child's correlated result or
            # progress event satisfies/refreshes the parent's expectation.
            if self.monitors is not None:
                self.monitors.observe(
                    correlation_id=event.get("correlation_id"),
                    source=event.get("source"),
                )
        for record in effects.get("arm", []):
            self.monitors.arm(record)
        for monitor_id in effects.get("disarm", []):
            self.monitors.disarm(monitor_id)
        self.store.set_effects(exec_id, None)
        if effects.get("publish"):
            # Fast-path wake: a publication may be the mail a parked
            # execution waits on. The loop tick remains the catch-all.
            self.wake_due_waits()

    def _apply_effects_guarded(self, exec_id: str, effects: Dict[str, Any]):
        """Apply effects without failing the already-committed node.

        A delivery failure after commit is an outbox problem, not a node
        problem: the transition is durable, so the node must not re-enter
        the retry path. Effects stay pending on the execution and are
        replayed at the next startup recovery.
        """
        try:
            self._apply_effects(exec_id, effects)
        except Exception:
            logger.exception(
                "Mailbox effects of %s failed to apply; they remain pending "
                "and will replay on recovery",
                exec_id,
            )

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

    def send_trigger(
        self,
        trigger: str,
        state: Dict[str, Any],
        serial_key: str = None,
    ):
        """
        Send a trigger message to the queue.

        This is what external services call — e.g. a Telegram listener.
        Triggers sharing ``serial_key`` never create overlapping sessions:
        while one is running, later triggers remain queued.  This is useful
        for conversation IDs, incident IDs, and other ordered event streams.
        """
        TriggerClient(self.queue).send_trigger(trigger, state, serial_key=serial_key)
        logger.info("Sent trigger '%s' to queue", trigger)

    # ── Main loop ─────────────────────────────────────────────

    def _handle_message(self, body: Dict):
        msg_type = body.get("type")

        not_before = body.get("not_before")
        if not_before and time.time() < not_before:
            # ReadyQueue intentionally has no timer contract yet. Requeueing
            # preserves the event on every backend; the short sleep prevents
            # a deferred trigger from becoming a hot loop locally.
            time.sleep(min(not_before - time.time(), 0.05))
            self.queue.send(body)
            return

        if msg_type == "trigger":
            serial_key = body.get("serial_key")
            session_id = self.start_session(
                body["trigger"], body.get("state", {}), serial_key=serial_key
            )
            if session_id is None:
                deferred = {**body, "not_before": time.time() + 0.1}
                self.queue.send(deferred)
                logger.debug("Deferred trigger for busy serial key %s", serial_key)
                return

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
        first empty poll with no parked executions — correct for
        single-process in-memory runs, where an empty queue plus nothing
        waiting means no work can ever arrive. Parked executions keep the
        loop alive: their timers and externally appended mail are checked
        at poll cadence by the wake tick.
        """
        logger.info("QueueRuntime starting...")

        # Crash recovery: check for any sessions that were running
        for session in self.store.get_running_sessions():
            logger.info("Recovering session %s", session["id"])
            # An execution still 'running' at startup is a worker that died
            # mid-node: nothing of its transition committed (the commit is
            # atomic), so return it to pending for re-execution. This assumes
            # one worker per store namespace — the current deployment shape;
            # multi-worker recovery needs leases (worker-hardening TODO).
            for ex in self.store.get_session_executions(
                session["id"], status="running"
            ):
                logger.warning(
                    "Re-queueing execution %s (%s) left running by a dead worker",
                    ex["id"], ex["node_name"],
                )
                self.store.mark_retrying(
                    ex["id"], error="recovered: worker stopped mid-execution"
                )
            self._enqueue_ready(session["id"])

        # Replay committed-but-unapplied mailbox effects (crash landed
        # between the transition commit and effect application, or a
        # delivery failed). The outbox index covers terminal sessions too —
        # a delivery failure does not stop a session, so pending effects
        # can outlive its completion. Application is idempotent, so
        # replaying after a partial apply is safe.
        for ex in self.store.get_pending_effect_executions():
            logger.warning(
                "Replaying pending mailbox effects of execution %s", ex["id"]
            )
            self._apply_effects_guarded(ex["id"], ex["effects"])

        self.tick_monitors()
        self.wake_due_waits()

        while True:
            messages = self.queue.receive(max_messages=10, wait_seconds=poll_wait)

            if not messages:
                # The wake tick: externally appended mail, expired wait
                # deadlines, and lapsed monitors have no queue message of
                # their own.
                lapsed = self.tick_monitors()
                if self.wake_due_waits() or lapsed:
                    continue
                self.collect_garbage()
                if stop_when_idle:
                    waiting = self.store.get_waiting_executions()
                    if not waiting:
                        logger.info("Queue idle, stopping.")
                        break
                    # Parked work can still wake (timer, external append) —
                    # idle means "nothing runnable now", not "nothing ever".
                    next_at = self._next_wake_at()
                    if next_at is not None:
                        time.sleep(max(0.0, min(next_at - time.time(), poll_wait)))
                continue

            self._process_messages(messages)
            self.tick_monitors()
            self.wake_due_waits()
            self.collect_garbage()

    def run_once(self, poll_wait: float = 1):
        """Process one batch of messages and return. Useful for testing."""
        messages = self.queue.receive(max_messages=10, wait_seconds=poll_wait)
        self._process_messages(messages)
        self.tick_monitors()
        self.wake_due_waits()
        self.collect_garbage()

    def collect_garbage(self, force: bool = False) -> list[str]:
        """Collect one bounded terminal-session batch when the interval is due."""
        if self.retention_policy is None:
            return []
        now = time.time()
        if not force and now - self._last_gc_at < self.retention_policy.interval_seconds:
            return []
        deleted = collect_terminal_sessions(self.store, self.retention_policy, now=now)
        self._last_gc_at = now
        if deleted:
            logger.info("Garbage-collected %d terminal session(s)", len(deleted))
        return deleted

    def _process_messages(self, messages):
        for message in messages:
            try:
                self._handle_message(message.payload)
                message.ack()
            except Exception as e:
                logger.exception("Error processing message: %s", e)
                message.nack()
