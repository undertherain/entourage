from entourage.builtin_capabilities import Facts, RecentSummaries, TopicShiftLifecycle
from entourage.capabilities import (
    Capability,
    CapabilityRegistry,
    ConversationLifecycle,
    Turn,
    TurnPlan,
)
from entourage.conversation import ContinuousAgent, ContinuousConversation, ConversationPolicy
from entourage.memory import ChatHistory

import pytest


class Section(Capability):
    def __init__(self, identifier, text):
        self.id = identifier
        self.text = text
        self.seen = []

    def prompt_section(self, turn):
        return self.text

    def after_turn(self, turn, reply):
        self.seen.append((turn.incoming, reply))


class Owner(ConversationLifecycle):
    def __init__(self, identifier="owner", plan=None):
        self.id = identifier
        self.plan = plan or TurnPlan()

    def before_turn(self, turn):
        return self.plan


class Topics:
    def __init__(self, summaries=(), shifts=False):
        self.summaries = list(summaries)
        self.shifts = shifts
        self.archived = []

    def recent_summaries(self):
        return self.summaries

    def is_new_topic(self, messages):
        return self.shifts

    def archive(self, messages):
        self.archived.append(messages)
        return "archive-1"


class Memory:
    def __init__(self, facts):
        self.facts = list(facts)

    def get_all(self):
        return self.facts


def conversation(tmp_path, topics=None, messages=()):
    history = ChatHistory("conversation", tmp_path)
    history.set_messages(list(messages))
    return ContinuousConversation(
        history,
        topics,
        ConversationPolicy(detect_topic_shifts=topics is not None, topic_carry_messages=2),
    )


def turn(incoming="hello"):
    return Turn(conversation_id="conversation", incoming=incoming)


# -- the contributive / exclusive split ------------------------------------


def test_contributive_capabilities_compose_in_registration_order():
    registry = CapabilityRegistry([Section("a", "first"), Section("b", "second")])

    assert registry.prompt_sections(turn()) == ["first", "second"]


def test_a_second_lifecycle_owner_is_a_construction_error():
    with pytest.raises(ValueError) as error:
        CapabilityRegistry([Owner("topics"), Section("facts", "f"), Owner("shift")])

    message = str(error.value)
    assert "exclusive" in message
    assert "topics" in message and "shift" in message


def test_duplicate_capability_ids_are_rejected():
    with pytest.raises(ValueError, match="duplicate capability id 'facts'"):
        CapabilityRegistry([Section("facts", "one"), Section("facts", "two")])


def test_registry_without_a_lifecycle_owner_leaves_history_alone():
    registry = CapabilityRegistry([Section("a", "first")])

    assert registry.lifecycle is None
    assert registry.before_turn(turn()) == TurnPlan()


def test_empty_sections_do_not_reach_the_prompt():
    registry = CapabilityRegistry([Section("a", "   "), Section("b", "kept")])

    assert registry.prompt_sections(turn()) == ["kept"]


# -- the durable history / model view split --------------------------------


def test_a_view_shrinks_the_prompt_without_shrinking_the_record(tmp_path):
    stored = [
        {"role": "user", "content": "old"},
        {"role": "assistant", "content": "older answer"},
    ]
    live = conversation(tmp_path, messages=stored)

    messages = live.messages_for("now", "system", view=[{"role": "user", "content": "digest"}])

    assert messages == [
        {"role": "system", "content": "system"},
        {"role": "user", "content": "digest"},
        {"role": "user", "content": "now"},
    ]
    kept = [item for item in live.history.get_messages() if item["role"] != "system"]
    assert kept == [*stored, {"role": "user", "content": "now"}]


def test_without_a_view_the_record_and_the_prompt_are_the_same(tmp_path):
    live = conversation(tmp_path, messages=[{"role": "user", "content": "old"}])

    messages = live.messages_for("now", "system")

    assert messages == live.history.get_messages()


# -- built-in capabilities --------------------------------------------------


def test_facts_render_one_section_and_disappear_when_empty():
    assert Facts(Memory([])).prompt_section(turn()) is None
    section = Facts(Memory(["[2026-01-01] drinks oolong"])).prompt_section(turn())
    assert section == "Facts you remember:\n- drinks oolong"


def test_recent_summaries_render_oldest_first():
    section = RecentSummaries(Topics(["newest", "oldest"])).prompt_section(turn())

    assert section.splitlines()[0].startswith("# Earlier topics")
    assert section.endswith("1. oldest\n\n2. newest")


def test_topic_shift_lifecycle_handles_reset_without_a_model_call(tmp_path):
    topics = Topics(shifts=False)
    live = conversation(tmp_path, topics, [{"role": "user", "content": "old"}])

    plan = TopicShiftLifecycle(live).before_turn(turn("/new"))

    assert plan.handled is True and plan.reply == "[fresh topic]"
    assert topics.archived == [[{"role": "user", "content": "old"}]]
    assert live.segment() == []


def test_topic_shift_lifecycle_archives_on_a_detected_shift(tmp_path):
    topics = Topics(shifts=True)
    live = conversation(tmp_path, topics, [{"role": "user", "content": "old"}])

    plan = TopicShiftLifecycle(live).before_turn(turn("unrelated"))

    assert plan.handled is False
    assert topics.archived == [[{"role": "user", "content": "old"}]]


# -- the agent loop consults the registry -----------------------------------


class StubRuntime:
    """Records what the model would have been sent, and answers once."""

    sent: list = []

    def __init__(self, debug=False):
        pass

    def start_session(self, agent, state):
        StubRuntime.sent.append(state["messages"])
        agent.history.append({"role": "assistant", "content": "answer"})

    def run(self):
        return None


def agent_with(tmp_path, capabilities, topics=None, messages=()):
    StubRuntime.sent = []
    live = conversation(tmp_path, topics, messages)
    return ContinuousAgent(
        "stub-model",
        [],
        live,
        lambda turn=None: "system",
        runtime_factory=StubRuntime,
        capabilities=CapabilityRegistry(capabilities),
    )


def test_a_handled_turn_short_circuits_the_model(tmp_path):
    owner = Owner(plan=TurnPlan(handled=True, reply="[fresh topic]"))
    loop = agent_with(tmp_path, [owner])

    assert loop.handle("/new") == "[fresh topic]"
    assert loop.conversation.history.get_messages() == []


def test_after_turn_observes_every_contributive_capability(tmp_path):
    section = Section("notes", "note")
    loop = agent_with(tmp_path, [section, Owner()])

    assert loop.handle("hello") == "answer"
    assert section.seen == [("hello", "answer")]


def test_composing_only_contributive_capabilities_keeps_built_in_reset(tmp_path):
    loop = agent_with(tmp_path, [Section("notes", "note")], messages=[{"role": "user", "content": "old"}])

    assert loop.handle("/new") == "[fresh topic]"
    assert loop.conversation.segment() == []


def test_a_lifecycle_view_reaches_the_model_but_not_the_record(tmp_path):
    owner = Owner(plan=TurnPlan(view=[{"role": "user", "content": "digest"}]))
    loop = agent_with(tmp_path, [owner], messages=[{"role": "user", "content": "old"}])

    loop.handle("now")

    assert StubRuntime.sent == [
        [
            {"role": "system", "content": "system"},
            {"role": "user", "content": "digest"},
            {"role": "user", "content": "now"},
        ]
    ]
    kept = [item["content"] for item in loop.conversation.segment()]
    assert kept == ["old", "now", "answer"]


def test_a_lifecycle_history_replacement_evicts_the_record(tmp_path):
    owner = Owner(plan=TurnPlan(history=[{"role": "user", "content": "kept"}]))
    loop = agent_with(tmp_path, [owner], messages=[{"role": "user", "content": "dropped"}])

    loop.handle("now")

    assert "dropped" not in [item["content"] for item in loop.conversation.segment()]
    assert StubRuntime.sent[0][1] == {"role": "user", "content": "kept"}
