"""Composable primitives for agents that live across many incoming turns."""

from dataclasses import dataclass
from typing import Callable, Optional

from .agent import PersistableAgent
from .memory import ChatHistory, TopicMemory, dialogue_tail
from .runtime import Runtime


@dataclass(frozen=True)
class ConversationPolicy:
    """Choices that define a continuous agent's conversation lifecycle."""

    detect_topic_shifts: bool = True
    reset_command: Optional[str] = "/new"
    # Dialogue messages carried into the next segment on a detected topic
    # shift, so a follow-up the judge misreads as a new topic keeps its
    # immediate context. The explicit reset command always clears everything.
    topic_carry_messages: int = 10


class ContinuousConversation:
    """History lifecycle spanning multiple Entourage execution sessions.

    The execution runtime currently schedules turns. This object supplies the
    longer-lived conversation identity and makes compaction/reset policy a
    reusable primitive rather than application glue.
    """

    def __init__(
        self,
        history: ChatHistory,
        topics: TopicMemory,
        policy: ConversationPolicy = ConversationPolicy(),
    ):
        self.history = history
        self.topics = topics
        self.policy = policy

    def segment(self) -> list[dict]:
        return [message for message in self.history.get_messages() if message.get("role") != "system"]

    def reset(self) -> bool:
        segment = self.segment()
        if segment:
            self.topics.archive(segment)
        self.history.set_messages([])
        return bool(segment)

    def begin_turn(self, incoming: str) -> bool:
        """Apply reset/topic-compaction policy before an incoming turn."""
        if self.policy.reset_command and incoming.strip() == self.policy.reset_command:
            self.reset()
            return True
        segment = self.segment()
        if not self.policy.detect_topic_shifts:
            return False
        if self.topics.is_new_topic(segment + [{"role": "user", "content": incoming}]):
            if segment:
                self.topics.archive(segment)
            self.history.set_messages(
                dialogue_tail(segment, self.policy.topic_carry_messages)
            )
            return True
        return False

    def messages_for(self, incoming: str, system_prompt: str) -> list[dict]:
        messages = [{"role": "system", "content": system_prompt}]
        messages.extend(self.segment())
        messages.append({"role": "user", "content": incoming})
        self.history.set_messages(messages)
        return messages


class ContinuousAgent:
    """A configurable main loop for an agent with continuous conversation."""

    def __init__(
        self,
        model: str,
        tools,
        conversation: ContinuousConversation,
        system_prompt: Callable[[], str],
        debug: bool = False,
        runtime_factory: Callable[..., Runtime] = Runtime,
        model_params: Optional[dict] = None,
    ):
        self.conversation = conversation
        self.system_prompt = system_prompt
        self.debug = debug
        self.runtime_factory = runtime_factory
        self.agent = PersistableAgent(
            model, tools, conversation.history, debug=debug, model_params=model_params
        )

    def handle(self, text: str) -> str:
        if self.conversation.policy.reset_command == text.strip():
            self.conversation.reset()
            return "[fresh topic]"
        self.conversation.begin_turn(text)
        self.conversation.messages_for(text, self.system_prompt())
        runtime = self.runtime_factory(debug=self.debug)
        runtime.start_session(self.agent, {"messages": self.conversation.history.get_messages()})
        runtime.run()
        for message in reversed(self.conversation.history.get_messages()):
            if message.get("role") == "assistant" and message.get("content"):
                return message["content"]
        return "(no reply)"
