"""Deployment configuration shared by Entourage applications and adapters."""

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping, Optional, Tuple


_ENV_VALUE = re.compile(r"^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$")


def resolve_value(value: Any, environ: Mapping[str, str] = os.environ) -> Any:
    """Resolve a whole-value ``${NAME}`` reference without interpolating secrets."""
    if isinstance(value, str):
        match = _ENV_VALUE.match(value)
        if match:
            return environ.get(match.group(1), "")
    return value


@dataclass(frozen=True)
class RedisRuntimeConfig:
    """One agent's namespaces on a shared Redis server.

    Redis is shared infrastructure; namespaces isolate schedulers whose workers
    do not necessarily register the same nodes.
    """

    url: str = "redis://localhost:6379/0"
    prefix: str = "entourage"

    @property
    def graph_namespace(self) -> str:
        return f"{self.prefix}:graph"

    @property
    def queue_namespace(self) -> str:
        return f"{self.prefix}:queue"

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "RedisRuntimeConfig":
        return cls(url=str(value.get("url", cls.url)), prefix=str(value.get("prefix", cls.prefix)))

    def graph_store(self):
        from .runtime import RedisGraphStore

        return RedisGraphStore(url=self.url, namespace=self.graph_namespace)

    def ready_queue(self):
        from .runtime import RedisReadyQueue

        return RedisReadyQueue(url=self.url, namespace=self.queue_namespace)

    @property
    def mailbox_namespace(self) -> str:
        return f"{self.prefix}:mailbox"

    def mailbox(self):
        from .redis_mailbox import RedisMailbox

        return RedisMailbox(url=self.url, namespace=self.mailbox_namespace)

    @property
    def monitors_namespace(self) -> str:
        return f"{self.prefix}:monitors"

    def monitors(self):
        from .monitors import RedisMonitorStore

        return RedisMonitorStore(url=self.url, namespace=self.monitors_namespace)

    def resources(self) -> "RuntimeResources":
        return RuntimeResources(
            graph_store=self.graph_store(),
            ready_queue=self.ready_queue(),
            mailbox=self.mailbox(),
            monitors=self.monitors(),
        )


@dataclass(frozen=True)
class RuntimeResources:
    """A coherent family of runtime backend strategies."""

    graph_store: Any
    ready_queue: Any
    mailbox: Any
    monitors: Any = None


@dataclass(frozen=True)
class RuntimeBackendConfig:
    """Select all runtime storage strategies with one backend setting."""

    backend: str = "memory"
    url: str = "redis://localhost:6379/0"
    prefix: str = "entourage"

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "RuntimeBackendConfig":
        return cls(
            backend=str(value.get("backend", "memory")),
            url=str(resolve_value(value.get("url", cls.url))),
            prefix=str(value.get("prefix", cls.prefix)),
        )

    def resources(self) -> RuntimeResources:
        if self.backend == "memory":
            from .mailbox import InMemoryMailbox
            from .monitors import InMemoryMonitorStore
            from .runtime import InMemoryGraphStore, InMemoryReadyQueue

            return RuntimeResources(
                graph_store=InMemoryGraphStore(),
                ready_queue=InMemoryReadyQueue(),
                mailbox=InMemoryMailbox(),
                monitors=InMemoryMonitorStore(),
            )
        if self.backend == "redis":
            return RedisRuntimeConfig(url=self.url, prefix=self.prefix).resources()
        raise ValueError(
            f"unknown runtime backend {self.backend!r}; expected 'memory' or 'redis'"
        )


@dataclass(frozen=True)
class ConversationConfig:
    topic_shift_detection: bool = True
    reset_command: str = "/new"
    recent_summary_limit: int = 3


@dataclass(frozen=True)
class AgentManifest:
    """Portable, application-owned declaration for one conversational agent."""

    id: str
    trigger: str
    runtime: RuntimeBackendConfig
    state_dir: Path
    model: str
    utility_model: str
    prompt: Path
    tools: Tuple[str, ...]
    manifest_path: Path
    setup: Optional[str] = None
    conversation: ConversationConfig = ConversationConfig()
    # Extra kwargs passed verbatim to every main-model completion call
    # (e.g. reasoning_effort for models that reject tools while reasoning).
    model_params: Mapping[str, Any] = field(default_factory=dict)

    @property
    def chat_dir(self) -> Path:
        return self.state_dir / "chats"

    @property
    def topic_archive_dir(self) -> Path:
        return self.state_dir / "topics"


def _relative_to_manifest(path: Any, manifest_path: Path) -> Path:
    result = Path(str(path))
    return result if result.is_absolute() else manifest_path.parent / result


def load_agent_manifest(path: Path, environ: Mapping[str, str] = os.environ) -> AgentManifest:
    """Load an agent manifest; relative files belong to the manifest directory."""
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - packaging guards this
        raise RuntimeError("PyYAML is required to load agent manifests") from exc

    manifest_path = Path(path).resolve()
    with manifest_path.open(encoding="utf-8") as stream:
        document = yaml.safe_load(stream) or {}
    raw = document.get("agent")
    if not isinstance(raw, Mapping):
        raise ValueError("manifest must contain an 'agent' mapping")

    conversation = raw.get("conversation") or {}
    if not isinstance(conversation, Mapping):
        raise ValueError("agent.conversation must be a mapping")
    tools = raw.get("tools") or []
    if not isinstance(tools, list) or not all(isinstance(item, str) for item in tools):
        raise ValueError("agent.tools must be a list of import strings")
    model_params = raw.get("model_params") or {}
    if not isinstance(model_params, Mapping):
        raise ValueError("agent.model_params must be a mapping")

    agent_id = str(raw["id"])
    runtime_raw = raw.get("runtime")
    if runtime_raw is not None and not isinstance(runtime_raw, Mapping):
        raise ValueError("agent.runtime must be a mapping")
    if runtime_raw is not None:
        runtime = RuntimeBackendConfig(
            backend=str(runtime_raw.get("backend", "memory")),
            url=str(
                resolve_value(
                    runtime_raw.get("url", "redis://localhost:6379/0"), environ
                )
            ),
            prefix=str(runtime_raw.get("prefix", f"entourage:{agent_id}")),
        )
    else:
        # Compatibility with the first manifest draft, which was Redis-only.
        runtime = RuntimeBackendConfig(
            backend="redis",
            url=str(
                resolve_value(
                    raw.get("redis_url", "redis://localhost:6379/0"), environ
                )
            ),
            prefix=str(raw.get("redis_prefix", f"entourage:{agent_id}")),
        )
    return AgentManifest(
        id=agent_id,
        trigger=str(raw.get("trigger", f"{agent_id}.message")),
        runtime=runtime,
        state_dir=_relative_to_manifest(raw.get("state_dir", "state"), manifest_path),
        model=str(resolve_value(raw["model"], environ)),
        utility_model=str(resolve_value(raw.get("utility_model", raw["model"]), environ)),
        prompt=_relative_to_manifest(raw.get("prompt", "prompt.md"), manifest_path),
        tools=tuple(tools),
        setup=str(raw["setup"]) if raw.get("setup") else None,
        manifest_path=manifest_path,
        model_params={
            str(key): resolve_value(value, environ)
            for key, value in model_params.items()
        },
        conversation=ConversationConfig(
            topic_shift_detection=bool(conversation.get("topic_shift_detection", True)),
            reset_command=str(conversation.get("reset_command", "/new")),
            recent_summary_limit=int(conversation.get("recent_summary_limit", 3)),
        ),
    )
