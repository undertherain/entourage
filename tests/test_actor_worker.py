"""Per-conversation mailbox actors: lazy spawn, ordering, rotation.

Runtime level: ``register_actor`` + ``ensure_actors`` keep a resident
session alive for any registered conversation with claimable mail — the
serial-key uniqueness check makes ensuring idempotent, and a completed or
failed session is replaced as soon as new mail arrives. Deployment level:
``MailboxAgentWorker`` turns drained batches into ordered, coalesced agent
turns with reset-command splitting and reply publication.
"""

from pathlib import Path

import pytest

from entourage.config import load_agent_manifest
from entourage.deployment import MailboxAgentWorker
from entourage.flow import Sequence, WaitForMailbox
from entourage.mailbox import InMemoryMailbox
from entourage.runtime import InMemoryGraphStore, InMemoryReadyQueue, QueueRuntime


@pytest.fixture
def mailbox():
    return InMemoryMailbox()


@pytest.fixture
def make_rt(store, mailbox):
    def factory(registry=None):
        return QueueRuntime(
            node_registry=registry or {},
            store=store,
            queue=InMemoryReadyQueue(),
            mailbox=mailbox,
        )

    return factory


def drain(rt, rounds=30):
    for _ in range(rounds):
        rt.run_once(poll_wait=0.01)


def make_consume(sink, loop=False):
    def consume(state):
        sink.append([event["event_id"] for event in state.get("events") or []])
        if loop:
            return state, Sequence(WaitForMailbox(limit=10), "consume")
        return state

    return consume


# ── Runtime: ensure_actors ────────────────────────────────────


def test_actor_spawns_lazily_and_drains_in_append_order(make_rt, mailbox, store):
    sink = []
    rt = make_rt({"consume": make_consume(sink)})
    rt.register_actor(
        "chat:", lambda _c: Sequence(WaitForMailbox(limit=10), "consume")
    )
    mailbox.append("chat:1", {"event_id": "e1", "kind": "user", "content": "one"})
    mailbox.append("chat:1", {"event_id": "e2", "kind": "user", "content": "two"})
    mailbox.append("corr:x", {"event_id": "other", "kind": "result"})
    drain(rt)

    # One actor turn saw both events, oldest first; acked inside the commit.
    assert sink == [["e1", "e2"]]
    assert mailbox.claimable_count("chat:1") == 0
    # A conversation outside the registered prefix gets no actor.
    assert mailbox.claimable_count("corr:x") == 1


def test_actor_is_singleton_while_alive_and_reused(make_rt, mailbox, store):
    sink = []
    rt = make_rt({"consume": make_consume(sink, loop=True)})
    rt.register_actor(
        "chat:", lambda _c: Sequence(WaitForMailbox(limit=10), "consume")
    )
    mailbox.append("chat:1", {"event_id": "e1", "kind": "user"})
    drain(rt)
    assert len(store.get_running_sessions()) == 1  # parked, still running

    mailbox.append("chat:1", {"event_id": "e2", "kind": "user"})
    drain(rt)

    assert sink == [["e1"], ["e2"]]
    assert len(store.get_running_sessions()) == 1  # same actor, not a twin


def test_actor_respawns_after_its_session_ends(make_rt, mailbox, store):
    sink = []
    rt = make_rt({"consume": make_consume(sink)})  # ends the session each turn
    rt.register_actor(
        "chat:", lambda _c: Sequence(WaitForMailbox(limit=10), "consume")
    )
    mailbox.append("chat:1", {"event_id": "e1", "kind": "user"})
    drain(rt)
    assert store.get_running_sessions() == []

    mailbox.append("chat:1", {"event_id": "e2", "kind": "user"})
    drain(rt)

    assert sink == [["e1"], ["e2"]]


def test_conversations_get_separate_actors(make_rt, mailbox, store):
    sink = []

    def consume(state):
        sink.append(
            (
                state.get("conversation_id"),
                [event["event_id"] for event in state.get("events") or []],
            )
        )
        return state

    rt = make_rt({"consume": consume})
    rt.register_actor(
        "chat:", lambda _c: Sequence(WaitForMailbox(limit=10), "consume")
    )
    mailbox.append("chat:1", {"event_id": "a1", "kind": "user"})
    mailbox.append("chat:2", {"event_id": "b1", "kind": "user"})
    drain(rt)

    assert sorted(sink) == [("chat:1", ["a1"]), ("chat:2", ["b1"])]


# ── Deployment: MailboxAgentWorker ────────────────────────────


class FakeAgent:
    created = []
    handled = []

    def __init__(self, manifest, conversation_id, context, debug):
        self.conversation_id = conversation_id
        type(self).created.append(conversation_id)

    def handle(self, text):
        type(self).handled.append((self.conversation_id, text))
        return f"re: {text}"


def write_manifest(tmp_path: Path) -> Path:
    path = tmp_path / "agent.yaml"
    path.write_text(
        """\
agent:
  id: kip
  state_dir: var
  model: test/model
  prompt: persona.md
  runtime:
    backend: memory
  conversation:
    reset_command: /new
""",
        encoding="utf-8",
    )
    return path


@pytest.fixture
def actor_app(tmp_path):
    FakeAgent.created = []
    FakeAgent.handled = []
    manifest = load_agent_manifest(write_manifest(tmp_path))
    mailbox = InMemoryMailbox()
    runtime = QueueRuntime(
        store=InMemoryGraphStore(),
        queue=InMemoryReadyQueue(),
        mailbox=mailbox,
    )
    published = []
    worker = MailboxAgentWorker(
        manifest,
        runtime,
        publisher=published.append,
        agent_factory=FakeAgent,
        conversation_prefix="chat:",
        rotate_after=2,
    )
    return runtime, mailbox, worker, published


def test_worker_coalesces_a_burst_into_one_ordered_turn(actor_app):
    runtime, mailbox, worker, published = actor_app
    mailbox.append(
        "chat:9",
        {"event_id": "m1", "kind": "user", "content": "one", "chat_id": "9"},
    )
    mailbox.append(
        "chat:9",
        {"event_id": "m2", "kind": "user", "content": "two", "chat_id": "9"},
    )
    drain(runtime)

    assert FakeAgent.handled == [("chat:9", "one\ntwo")]
    assert [(p["chat_id"], p["reply"]) for p in published] == [("9", "re: one\ntwo")]


def test_worker_survives_rotation_without_losing_order(actor_app):
    runtime, mailbox, worker, published = actor_app  # rotate_after=2
    for index in range(5):
        mailbox.append(
            "chat:9",
            {
                "event_id": f"m{index}",
                "kind": "user",
                "content": f"msg {index}",
                "chat_id": "9",
            },
        )
        drain(runtime)

    texts = [text for _c, text in FakeAgent.handled]
    assert texts == [f"msg {index}" for index in range(5)]
    # Sessions rotated at least once along the way…
    actors = [
        s
        for s in runtime.store.get_terminal_sessions()
        if s["trigger"].startswith("actor:")
    ]
    assert len(actors) >= 2
    # …but the conversation identity (and thus the agent) stayed the same.
    assert FakeAgent.created == ["chat:9"]


def test_worker_reset_command_travels_alone_and_bare(actor_app):
    runtime, mailbox, worker, published = actor_app
    events = [
        {"event_id": "1", "kind": "user", "content": "hello"},
        {"event_id": "2", "kind": "user", "content": " /new "},
        {"event_id": "3", "kind": "user", "content": "fresh start"},
        {"event_id": "4", "kind": "system", "source": "timer"},
    ]

    assert worker.turn_texts("chat:9", events) == ["hello", "/new", "fresh start"]


def test_worker_timer_only_batch_makes_no_agent_turn(actor_app):
    runtime, mailbox, worker, published = actor_app
    state = worker.handle_turn(
        {
            "conversation_id": "chat:9",
            "events": [{"kind": "system", "source": "timer"}],
        }
    )

    assert state["replies"] == []
    assert FakeAgent.handled == []
    worker.publish_replies(state)
    assert published == []
