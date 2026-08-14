from pathlib import Path

import pytest

from entourage.config import load_agent_manifest
from entourage.deployment import AgentWorker, import_object


class FakeRuntime:
    def __init__(self):
        self.nodes = {}
        self.pipelines = {}

    def register_node(self, name, function, **policy):
        self.nodes[name] = (function, policy)

    def register_pipeline(self, name, function):
        self.pipelines[name] = function


class FakeAgent:
    created = []

    def __init__(self, manifest, conversation_id, context, debug):
        self.conversation_id = conversation_id
        self.created.append(conversation_id)

    def handle(self, text):
        return f"{self.conversation_id}: {text}"


def write_manifest(tmp_path: Path) -> Path:
    path = tmp_path / "agent.yaml"
    path.write_text(
        """\
agent:
  id: kip
  trigger: kip.message
  redis_url: ${TEST_REDIS_URL}
  redis_prefix: agents:kip
  state_dir: var
  model: ${TEST_MODEL}
  utility_model: utility
  prompt: persona.md
  tools: []
  conversation:
    topic_shift_detection: false
    reset_command: /clear
    recent_summary_limit: 2
""",
        encoding="utf-8",
    )
    return path


def test_manifest_is_portable_and_resolves_whole_environment_values(tmp_path):
    path = write_manifest(tmp_path)
    manifest = load_agent_manifest(
        path, {"TEST_REDIS_URL": "redis://example/2", "TEST_MODEL": "test/model"}
    )

    assert manifest.id == "kip"
    assert manifest.runtime.url == "redis://example/2"
    assert manifest.state_dir == tmp_path / "var"
    assert manifest.prompt == tmp_path / "persona.md"
    assert manifest.model == "test/model"
    assert manifest.conversation.topic_shift_detection is False
    assert manifest.conversation.reset_command == "/clear"


def test_worker_keeps_conversations_separate_and_publishes(tmp_path):
    FakeAgent.created = []
    manifest = load_agent_manifest(write_manifest(tmp_path), {"TEST_MODEL": "model"})
    runtime = FakeRuntime()
    published = []
    worker = AgentWorker(manifest, runtime, publisher=published.append, agent_factory=FakeAgent)

    first = worker.handle_event({"conversation_id": "a", "text": "one"})
    second = worker.handle_event({"conversation_id": "b", "text": "two"})
    again = worker.handle_event({"conversation_id": "a", "text": "three"})
    worker.publish_reply(first)

    assert [first["reply"], second["reply"], again["reply"]] == [
        "a: one", "b: two", "a: three"
    ]
    assert FakeAgent.created == ["a", "b"]
    assert published == [first]
    assert set(runtime.nodes) == {"kip.handle_event", "kip.publish_reply"}
    assert set(runtime.pipelines) == {"kip.message"}


def test_import_object_rejects_ambiguous_reference():
    with pytest.raises(ValueError, match="module:object"):
        import_object("not_a_reference")
