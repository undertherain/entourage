"""Composable primitives for agents that live across many incoming turns."""

import inspect
from dataclasses import dataclass
from typing import Callable, Optional

from .agent import PersistableAgent
from .capabilities import CapabilityRegistry, Turn, TurnPlan
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
        topics: Optional[TopicMemory] = None,
        policy: ConversationPolicy = ConversationPolicy(),
    ):
        self.history = history
        self.topics = topics
        self.policy = policy

    def segment(self) -> list[dict]:
        return [message for message in self.history.get_messages() if message.get("role") != "system"]

    def reset(self) -> bool:
        segment = self.segment()
        if segment and self.topics is not None:
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
        if self.topics is not None and self.topics.is_new_topic(
            segment + [{"role": "user", "content": incoming}]
        ):
            if segment:
                self.topics.archive(segment)
            self.history.set_messages(
                dialogue_tail(segment, self.policy.topic_carry_messages)
            )
            return True
        return False

    def messages_for(
        self,
        incoming: str,
        system_prompt: str,
        view: Optional[list[dict]] = None,
    ) -> list[dict]:
        """Persist the durable turn; return what the model should see.

        ``view`` lets a lifecycle capability trim, summarize, or evict for one
        call without dropping anything from the stored conversation, so the
        durable record and the model's window can diverge deliberately.
        """
        durable = [{"role": "system", "content": system_prompt}]
        durable.extend(self.segment())
        durable.append({"role": "user", "content": incoming})
        self.history.set_messages(durable)
        if view is None:
            return durable
        return [
            {"role": "system", "content": system_prompt},
            *view,
            {"role": "user", "content": incoming},
        ]


class ContinuousAgent:
    """A configurable main loop for an agent with continuous conversation."""

    def __init__(
        self,
        model: str,
        tools,
        conversation: ContinuousConversation,
        system_prompt: Callable[..., str],
        debug: bool = False,
        runtime_factory: Callable[..., Runtime] = Runtime,
        model_params: Optional[dict] = None,
        capabilities: Optional[CapabilityRegistry] = None,
    ):
        self.conversation = conversation
        self.system_prompt = system_prompt
        self.debug = debug
        self.runtime_factory = runtime_factory
        self.capabilities = capabilities
        # A prompt builder may accept the turn under assembly; older callers
        # pass a plain zero-argument callable and keep working unchanged.
        self._prompt_takes_turn = bool(inspect.signature(system_prompt).parameters)
        self.agent = PersistableAgent(
            model, tools, conversation.history, debug=debug, model_params=model_params
        )

    def turn_for(self, text: str) -> Turn:
        return Turn(
            conversation_id=self.conversation.history.chat_id,
            incoming=text,
            segment=self.conversation.segment(),
        )

    def _system_prompt(self, turn: Turn) -> str:
        return self.system_prompt(turn) if self._prompt_takes_turn else self.system_prompt()

    def handle(self, text: str) -> str:
        turn = self.turn_for(text)
        plan = (
            self.capabilities.before_turn(turn)
            if self.capabilities is not None
            else TurnPlan()
        )
        if plan.handled:
            return plan.reply
        if self.capabilities is None or self.capabilities.lifecycle is None:
            # No history owner composed: keep the built-in reset/shift policy,
            # so composing purely contributive capabilities changes nothing.
            if self.conversation.policy.reset_command == text.strip():
                self.conversation.reset()
                return "[fresh topic]"
            self.conversation.begin_turn(text)
        if plan.history is not None:
            self.conversation.history.set_messages(plan.history)
        messages = self.conversation.messages_for(
            text, self._system_prompt(turn), view=plan.view
        )
        runtime = self.runtime_factory(debug=self.debug)
        runtime.start_session(self.agent, {"messages": messages})
        runtime.run()
        reply = "(no reply)"
        for message in reversed(self.conversation.history.get_messages()):
            if message.get("role") == "assistant" and message.get("content"):
                reply = message["content"]
                break
        if self.capabilities is not None:
            self.capabilities.after_turn(turn, reply)
        return reply
