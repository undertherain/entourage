import json

from entourage.conversation import ContinuousConversation, ConversationPolicy
from entourage.memory import ChatHistory, EventHistory, conversation_storage_key


class Topics:
    def __init__(self, shifts=False):
        self.shifts = shifts
        self.archived = []

    def is_new_topic(self, messages):
        self.probe = messages
        return self.shifts

    def archive(self, messages):
        self.archived.append(messages)


def history(tmp_path, messages):
    value = ChatHistory("conversation", tmp_path)
    value.set_messages(messages)
    return value


def test_conversation_id_cannot_escape_history_directory(tmp_path):
    value = ChatHistory("../../outside", tmp_path)
    value.set_messages([{"role": "user", "content": "safe"}])

    assert value.file_path.parent == tmp_path
    assert value.file_path.name == "..%2F..%2Foutside.json"
    assert conversation_storage_key("telegram-main:123") == "telegram-main:123"


def test_event_history_is_idempotent_and_reloadable(tmp_path):
    history = EventHistory("group:42", tmp_path)
    event = {"event_id": "telegram:7", "kind": "user", "content": "hello"}

    assert history.append(event) is True
    assert history.append({**event, "content": "duplicate"}) is False
    assert EventHistory("group:42", tmp_path).get_events() == [event]


def test_event_history_rotates_old_events_to_append_only_archive(tmp_path):
    history = EventHistory("group:42", tmp_path, max_events=2)
    for number in range(3):
        history.append({"event_id": str(number), "content": f"event {number}"})

    assert [event["event_id"] for event in history.get_events()] == ["1", "2"]
    archived = [
        json.loads(line)
        for line in history.archive_path.read_text(encoding="utf-8").splitlines()
    ]
    assert [event["event_id"] for event in archived] == ["0"]


def test_topic_shift_archives_segment_and_keeps_dialogue_tail(tmp_path):
    segment = [
        {"role": "user", "content": "old question"},
        {"role": "assistant", "content": None, "tool_calls": [{"id": "call_1"}]},
        {"role": "tool", "content": "raw tool output"},
        {"role": "assistant", "content": "old answer"},
    ]
    chat = history(tmp_path, segment)
    topics = Topics(shifts=True)
    conversation = ContinuousConversation(
        chat, topics, ConversationPolicy(topic_carry_messages=2)
    )

    assert conversation.begin_turn("new topic") is True
    assert topics.archived == [segment]
    assert chat.get_messages() == [
        {"role": "user", "content": "old question"},
        {"role": "assistant", "content": "old answer"},
    ]


def test_topic_shift_with_zero_carry_clears_history(tmp_path):
    chat = history(tmp_path, [{"role": "user", "content": "old topic"}])
    topics = Topics(shifts=True)
    conversation = ContinuousConversation(
        chat, topics, ConversationPolicy(topic_carry_messages=0)
    )

    assert conversation.begin_turn("new topic") is True
    assert topics.archived == [[{"role": "user", "content": "old topic"}]]
    assert chat.get_messages() == []


def test_manual_reset_uses_same_compaction_path(tmp_path):
    chat = history(tmp_path, [{"role": "assistant", "content": "finished"}])
    topics = Topics()
    conversation = ContinuousConversation(chat, topics)

    assert conversation.begin_turn("/new") is True
    assert topics.archived == [[{"role": "assistant", "content": "finished"}]]
    assert chat.get_messages() == []


def test_topic_detection_can_be_disabled(tmp_path):
    chat = history(tmp_path, [{"role": "user", "content": "old topic"}])
    topics = Topics(shifts=True)
    conversation = ContinuousConversation(
        chat, topics, ConversationPolicy(detect_topic_shifts=False)
    )

    assert conversation.begin_turn("new topic") is False
    assert topics.archived == []
    assert chat.get_messages()


def test_messages_for_rebuilds_system_prompt_without_losing_segment(tmp_path):
    chat = history(tmp_path, [{"role": "user", "content": "earlier"}])
    conversation = ContinuousConversation(chat, Topics())

    messages = conversation.messages_for("now", "persona")

    assert messages == [
        {"role": "system", "content": "persona"},
        {"role": "user", "content": "earlier"},
        {"role": "user", "content": "now"},
    ]
