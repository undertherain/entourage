"""Manifest-driven conversational agents and durable trigger workers."""

import importlib
from pathlib import Path
from typing import Any, Callable, List, Optional

from .config import AgentManifest, load_agent_manifest
from .conversation import ContinuousAgent, ContinuousConversation, ConversationPolicy
from .flow import Sequence, WaitForMailbox
from .memory import ChatHistory, TopicMemory, conversation_storage_key
from .runtime import QueueRuntime


def import_object(reference: str) -> Any:
    """Import ``package.module:object`` with a useful configuration error."""
    module_name, separator, object_name = reference.partition(":")
    if not separator or not module_name or not object_name:
        raise ValueError(f"invalid import reference {reference!r}; expected module:object")
    module = importlib.import_module(module_name)
    try:
        return getattr(module, object_name)
    except AttributeError as exc:
        raise ValueError(f"{reference!r} does not exist") from exc


def load_tools(manifest: AgentManifest, context: Any = None) -> list[Any]:
    """Instantiate manifest tools. Factories receive setup context when present."""
    result = []
    for reference in manifest.tools:
        factory = import_object(reference)
        result.append(factory(context) if context is not None else factory())
    return result


class ConfiguredAgent:
    """Generic continuous agent assembled entirely from an application manifest."""

    def __init__(
        self,
        manifest: AgentManifest,
        conversation_id: str,
        context: Any = None,
        debug: bool = False,
    ):
        manifest.chat_dir.mkdir(parents=True, exist_ok=True)
        history = ChatHistory(conversation_id, manifest.chat_dir)
        topics = TopicMemory(
            manifest.topic_archive_dir / conversation_storage_key(conversation_id),
            manifest.utility_model,
            manifest.conversation.recent_summary_limit,
        )
        conversation = ContinuousConversation(
            history,
            topics,
            ConversationPolicy(
                detect_topic_shifts=manifest.conversation.topic_shift_detection,
                reset_command=manifest.conversation.reset_command,
                topic_carry_messages=manifest.conversation.topic_carry_messages,
            ),
        )
        self._manifest = manifest
        self._topics = topics
        self._context = context
        self._loop = ContinuousAgent(
            manifest.model,
            load_tools(manifest, context),
            conversation,
            self.system_prompt,
            debug=debug,
            model_params=dict(manifest.model_params),
        )

    def system_prompt(self) -> str:
        base = self._manifest.prompt.read_text(encoding="utf-8")
        summaries = self._topics.recent_summaries()
        if not summaries:
            return base
        numbered = "\n\n".join(
            f"{index}. {summary.strip()}"
            for index, summary in enumerate(reversed(summaries), start=1)
        )
        return (
            base
            + "\n\n# Earlier topics in this conversation (summaries, oldest first)\n\n"
            + numbered
        )

    def handle(self, text: str) -> str:
        return self._loop.handle(text)


class AgentWorker:
    """Bind a conversational agent to a QueueRuntime trigger pipeline.

    Transport-specific delivery stays outside the worker. A publisher callback
    may write a Redis Stream, call a test spy, or hand the reply to any adapter.
    """

    def __init__(
        self,
        manifest: AgentManifest,
        runtime: QueueRuntime,
        publisher: Optional[Callable[[dict], None]] = None,
        agent_factory: Callable[..., Any] = ConfiguredAgent,
        debug: bool = False,
    ):
        self.manifest = manifest
        self.runtime = runtime
        self.publisher = publisher
        self.agent_factory = agent_factory
        self.debug = debug
        self.context = import_object(manifest.setup)(manifest) if manifest.setup else None
        self.conversations: dict[str, Any] = {}
        self._register()

    def _agent(self, conversation_id: str) -> Any:
        if conversation_id not in self.conversations:
            self.conversations[conversation_id] = self.agent_factory(
                self.manifest, conversation_id, self.context, self.debug
            )
        return self.conversations[conversation_id]

    def handle_event(self, state: dict) -> dict:
        text = state.get("text")
        if not isinstance(text, str) or not text.strip():
            state["reply"] = ""
            return state
        conversation_id = str(state.get("conversation_id", "main"))
        state["reply"] = self._agent(conversation_id).handle(text)
        return state

    def publish_reply(self, state: dict) -> dict:
        if self.publisher is not None and state.get("reply"):
            self.publisher(state)
        return state

    def _register(self) -> None:
        handle = f"{self.manifest.id}.handle_event"
        publish = f"{self.manifest.id}.publish_reply"
        self.runtime.register_node(handle, self.handle_event)
        self.runtime.register_node(publish, self.publish_reply, max_attempts=3, retry_delay=2)
        self.runtime.register_pipeline(
            self.manifest.trigger, lambda _state: Sequence(handle, publish)
        )


class MailboxAgentWorker:
    """Bind a conversational agent to per-conversation mailbox actors.

    The trigger-per-message shape (``AgentWorker``) leaves ordering to the
    ready queue: triggers deferred behind a busy ``serial_key`` re-enter the
    shared queue and can overtake each other. Here each conversation owns a
    resident actor session looping ``WaitForMailbox → handle_turn →
    publish_replies → re-park``, so pending messages are consumed strictly
    in mailbox append order, a burst drained together is coalesced into one
    agent turn, and drained events are acknowledged inside the transition
    commit that incorporates them.

    Transports only append events to the runtime's mailbox (idempotently,
    by ``event_id``); ``QueueRuntime.register_actor`` creates and revives
    the per-conversation sessions lazily whenever claimable mail exists —
    including after a rotation or a failed session. Sessions rotate (end
    and get respawned by the engine) every ``rotate_after`` turns so the
    execution graph stays bounded well below the session node limit.

    Events with ``kind: "user"`` reach the agent; a ``formatter(
    conversation_id, events)`` callback renders one drained group into the
    agent's incoming text (default: contents joined by newlines). The
    conversation reset command always travels alone: it splits a batch and
    is passed to the agent bare, preserving ``ContinuousConversation``'s
    reset semantics. Other kinds (``system`` timer events and the like)
    produce no agent turn yet — the actor just re-parks.
    """

    def __init__(
        self,
        manifest: AgentManifest,
        runtime: QueueRuntime,
        publisher: Optional[Callable[[dict], None]] = None,
        agent_factory: Callable[..., Any] = ConfiguredAgent,
        formatter: Optional[Callable[[str, List[dict]], Optional[str]]] = None,
        conversation_prefix: Optional[str] = None,
        batch_limit: int = 10,
        rotate_after: int = 200,
        debug: bool = False,
    ):
        if batch_limit < 1:
            raise ValueError("batch_limit must be >= 1")
        if rotate_after < 1:
            raise ValueError("rotate_after must be >= 1")
        self.manifest = manifest
        self.runtime = runtime
        self.publisher = publisher
        self.agent_factory = agent_factory
        self.formatter = formatter
        self.batch_limit = batch_limit
        self.rotate_after = rotate_after
        self.debug = debug
        self.context = import_object(manifest.setup)(manifest) if manifest.setup else None
        self.conversations: dict[str, Any] = {}
        self._handle_name = f"{manifest.id}.handle_turn"
        self._publish_name = f"{manifest.id}.publish_replies"
        self.runtime.register_node(self._handle_name, self.handle_turn)
        self.runtime.register_node(
            self._publish_name, self.publish_replies, max_attempts=3, retry_delay=2
        )
        self.runtime.register_actor(
            conversation_prefix or f"{manifest.id}:", lambda _conversation: self.plan()
        )

    def plan(self) -> Sequence:
        """One turn of the actor loop; ``publish_replies`` splices the next."""
        return Sequence(
            WaitForMailbox(limit=self.batch_limit),
            self._handle_name,
            self._publish_name,
        )

    def _agent(self, conversation_id: str) -> Any:
        if conversation_id not in self.conversations:
            self.conversations[conversation_id] = self.agent_factory(
                self.manifest, conversation_id, self.context, self.debug
            )
        return self.conversations[conversation_id]

    def _format(self, conversation_id: str, events: List[dict]) -> Optional[str]:
        if self.formatter is not None:
            return self.formatter(conversation_id, events)
        text = "\n".join(
            str(event.get("content", "")) for event in events
        ).strip()
        return text or None

    def turn_texts(self, conversation_id: str, events: List[dict]) -> List[str]:
        """Render one drained batch into ordered agent inputs.

        Consecutive user events coalesce into one text; the reset command
        breaks the batch and goes through alone and bare.
        """
        reset = self.manifest.conversation.reset_command
        texts: List[str] = []
        group: List[dict] = []

        def flush():
            if group:
                text = self._format(conversation_id, list(group))
                if text:
                    texts.append(text)
                group.clear()

        for event in events:
            if event.get("kind") != "user":
                continue
            if reset and str(event.get("content", "")).strip() == reset:
                flush()
                texts.append(reset)
            else:
                group.append(event)
        flush()
        return texts

    def handle_turn(self, state: dict) -> dict:
        events = state.get("events") or []
        conversation_id = str(state.get("conversation_id", "main"))
        chat_id = next(
            (
                str(event["chat_id"])
                for event in reversed(events)
                if event.get("chat_id")
            ),
            state.get("chat_id"),
        )
        agent = self._agent(conversation_id)
        replies = [
            agent.handle(text)
            for text in self.turn_texts(conversation_id, events)
        ]
        return {**state, "chat_id": chat_id, "events": [], "replies": replies}

    def publish_replies(self, state: dict):
        if self.publisher is not None:
            for reply in state.get("replies") or []:
                if reply:
                    self.publisher({**state, "reply": reply})
        turns = int(state.get("turns", 0)) + 1
        next_state = {**state, "replies": [], "turns": turns}
        if turns >= self.rotate_after:
            # Rotation: end this session; the engine's ensure tick spawns a
            # fresh actor as soon as the conversation has claimable mail.
            return next_state
        return next_state, self.plan()


def create_worker(
    manifest_path: Path,
    publisher: Optional[Callable[[dict], None]] = None,
    runtime_factory: Callable[..., QueueRuntime] = QueueRuntime,
) -> AgentWorker:
    manifest = load_agent_manifest(manifest_path)
    resources = manifest.runtime.resources()
    runtime = runtime_factory(
        store=resources.graph_store,
        queue=resources.ready_queue,
        mailbox=resources.mailbox,
    )
    return AgentWorker(manifest, runtime, publisher=publisher)
