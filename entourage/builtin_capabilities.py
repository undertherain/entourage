"""Capabilities that ship with Entourage.

These are ``ConfiguredAgent``'s historical behaviours, extracted so that an
application can keep them, reorder them, or replace them outright instead of
inheriting them. Nothing here is privileged: they use exactly the hooks an
application capability uses, and an agent that composes none of them is a
supported configuration.
"""

from typing import Optional

from .capabilities import Capability, ConversationLifecycle, Turn, TurnPlan


class Facts(Capability):
    """Durable operator facts from a ``MemoryDB``, rendered as one section."""

    id = "facts"

    def __init__(self, memory, heading: str = "Facts you remember:"):
        self.memory = memory
        self.heading = heading

    def prompt_section(self, turn: Turn) -> Optional[str]:
        facts = self.memory.get_all()
        if not facts:
            return None
        lines = [fact.split("] ", 1)[1] if "] " in fact else fact for fact in facts]
        return "\n".join([self.heading, *(f"- {line}" for line in lines)])


class RecentSummaries(Capability):
    """The last few archived segment summaries, oldest first.

    This is recency across the whole archive, not the current subject. An agent
    that tracks topics explicitly should *replace* this rather than compose
    both: two accounts of "what we were discussing" reach the model together
    and the unfocused one bleeds unrelated context into a focused turn.
    """

    id = "recent_summaries"
    heading = "# Earlier topics in this conversation (summaries, oldest first)"

    def __init__(self, topics):
        self.topics = topics

    def prompt_section(self, turn: Turn) -> Optional[str]:
        summaries = self.topics.recent_summaries()
        if not summaries:
            return None
        numbered = "\n\n".join(
            f"{index}. {summary.strip()}"
            for index, summary in enumerate(reversed(summaries), start=1)
        )
        return f"{self.heading}\n\n{numbered}"


class TopicShiftLifecycle(ConversationLifecycle):
    """Archive the live segment whenever a cheap judge calls the topic new.

    Entourage's default history owner. It delegates to
    ``ContinuousConversation``'s policy, so reset handling and the carried
    dialogue tail keep their existing semantics.
    """

    id = "topic_shift"

    def __init__(self, conversation, reset_reply: str = "[fresh topic]"):
        self.conversation = conversation
        self.reset_reply = reset_reply

    def before_turn(self, turn: Turn) -> TurnPlan:
        reset = self.conversation.policy.reset_command
        if reset and turn.incoming.strip() == reset:
            self.conversation.reset()
            return TurnPlan(handled=True, reply=self.reset_reply)
        self.conversation.begin_turn(turn.incoming)
        return TurnPlan()
