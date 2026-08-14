from entourage.conversation import ContinuousConversation, ConversationPolicy
from entourage.memory import ChatHistory


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


def test_topic_shift_archives_segment_and_clears_history(tmp_path):
    chat = history(tmp_path, [{"role": "user", "content": "old topic"}])
    topics = Topics(shifts=True)
    conversation = ContinuousConversation(chat, topics)

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
