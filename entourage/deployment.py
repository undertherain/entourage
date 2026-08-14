"""Manifest-driven conversational agents and durable trigger workers."""

import importlib
from pathlib import Path
from typing import Any, Callable, Optional

from .config import AgentManifest, load_agent_manifest
from .conversation import ContinuousAgent, ContinuousConversation, ConversationPolicy
from .flow import Sequence
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
            ),
        )
        self._manifest = manifest
        self._context = context
        self._loop = ContinuousAgent(
            manifest.model,
            load_tools(manifest, context),
            conversation,
            self.system_prompt,
            debug=debug,
        )

    def system_prompt(self) -> str:
        return self._manifest.prompt.read_text(encoding="utf-8")

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


def create_worker(
    manifest_path: Path,
    publisher: Optional[Callable[[dict], None]] = None,
    runtime_factory: Callable[..., QueueRuntime] = QueueRuntime,
) -> AgentWorker:
    manifest = load_agent_manifest(manifest_path)
    runtime = runtime_factory(
        store=manifest.runtime.graph_store(), queue=manifest.runtime.ready_queue()
    )
    return AgentWorker(manifest, runtime, publisher=publisher)
