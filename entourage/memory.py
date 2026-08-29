import json
import datetime
import uuid
import threading
from pathlib import Path
from urllib.parse import quote

from litellm import completion


TOPIC_JUDGE_PROMPT = """\
You are a topic detection system. Analyze the last message in the context of
the recent conversation history. Does it introduce a significantly new topic?

Respond with only the single word YES or NO.

Recent conversation history:
{history}

Last message:
{last}
"""

TOPIC_SUMMARIZER_PROMPT = """\
Create a concise, one-paragraph summary of this conversation. Capture the key
topics, questions, conclusions, decisions, and actions taken.
"""


def _dialogue_only(messages: list[dict]) -> list[dict]:
    return [
        {"role": message["role"], "content": message["content"]}
        for message in messages
        if message.get("role") in ("user", "assistant") and message.get("content")
    ]


def dialogue_tail(messages: list[dict], limit: int) -> list[dict]:
    """Last ``limit`` dialogue messages, valid as a standalone transcript.

    Tool traffic is dropped: a tool result or a tool_calls stub cannot open a
    transcript, and the assistant's answers already restate what tools said.
    """
    if limit <= 0:
        return []
    return _dialogue_only(messages)[-limit:]


def conversation_storage_key(conversation_id: str) -> str:
    """Encode an external conversation id as one safe, readable path segment."""
    return quote(str(conversation_id), safe="-_.:")

class ChatHistory:
    """Manages loading, saving, and accessing conversation messages for a single chat session."""

    def __init__(self, chat_id: str, history_dir: Path):
        self.chat_id = chat_id
        self.history_dir = history_dir
        self.file_path = self.history_dir / f"{conversation_storage_key(self.chat_id)}.json"
        self.messages: list[dict] = []
        self._load()

    def _load(self):
        """Loads messages from the chat file if it exists."""
        try:
            # self.file_path.touch(exist_ok=True) # Don't touch, just check exists
            if self.file_path.exists():
                with open(self.file_path, "r", encoding="utf-8") as f:
                    content = f.read()
                    if content:
                        self.messages = json.loads(content)
        except (json.JSONDecodeError, IOError) as e:
            print(f"Warning: Could not load chat history for {self.chat_id}: {e}")
            self.messages = []

    def _save(self):
        """Saves the current messages to the chat file."""
        try:
            self.history_dir.mkdir(parents=True, exist_ok=True)
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump(self.messages, f, ensure_ascii=False, indent=2)
        except IOError as e:
            print(f"Warning: Failed to save chat to {self.file_path}: {e}")

    def append(self, message: dict):
        """Appends a message to the history and automatically saves."""
        message_with_timestamp = {
            **message,
            "timestamp": datetime.datetime.now().isoformat(),
        }
        self.messages.append(message_with_timestamp)
        self._save()

    def get_messages(self) -> list[dict]:
        """Returns the list of messages."""
        return self.messages

    def set_messages(self, messages: list[dict]):
        """Directly sets the message list and saves."""
        self.messages = messages
        self._save()

    def start_new_session(self):
        """Starts a new session with a new ID."""
        self.chat_id = str(uuid.uuid4())
        self.file_path = self.history_dir / f"{self.chat_id}.json"
        self.messages = []
        self._save()
        return self.chat_id


class EventHistory:
    """Durable, idempotent typed conversation events for one conversation."""

    def __init__(
        self, conversation_id: str, history_dir: Path, max_events: int = 1000
    ):
        if max_events < 1:
            raise ValueError("max_events must be >= 1")
        self.conversation_id = conversation_id
        self.history_dir = Path(history_dir)
        self.file_path = self.history_dir / f"{conversation_storage_key(conversation_id)}.events.json"
        self.archive_path = self.history_dir / (
            f"{conversation_storage_key(conversation_id)}.events.archive.jsonl"
        )
        self.max_events = max_events
        self._lock = threading.Lock()
        self._events: list[dict] = []
        if self.file_path.exists():
            try:
                self._events = json.loads(self.file_path.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError) as exc:
                raise RuntimeError(f"could not load event history {self.file_path}: {exc}") from exc

    def append(self, event: dict) -> bool:
        """Persist an event once by event_id; return whether it was appended."""
        event_id = event.get("event_id")
        if not event_id:
            raise ValueError("event_id is required")
        with self._lock:
            if any(item.get("event_id") == event_id for item in self._events):
                return False
            self._events.append(dict(event))
            self.history_dir.mkdir(parents=True, exist_ok=True)
            if len(self._events) > self.max_events:
                archived = self._events[: len(self._events) - self.max_events]
                with self.archive_path.open("a", encoding="utf-8") as stream:
                    for item in archived:
                        stream.write(json.dumps(item, ensure_ascii=False) + "\n")
                self._events = self._events[-self.max_events :]
            temporary = self.file_path.with_suffix(self.file_path.suffix + ".tmp")
            temporary.write_text(
                json.dumps(self._events, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            temporary.replace(self.file_path)
        return True

    def get_events(self) -> list[dict]:
        with self._lock:
            return [dict(event) for event in self._events]


class MemoryDB:
    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.file_path.touch(exist_ok=True)

    def add(self, fact: str):
        timestamp = datetime.datetime.now().isoformat()
        with open(self.file_path, "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] {fact}\n")

    def get_all(self) -> list[str]:
        with open(self.file_path, "r", encoding="utf-8") as f:
            return [line.strip() for line in f.readlines()]


class TopicMemory:
    """LLM-assisted topic segmentation with a portable file archive.

    Applications decide when to call it and how summaries enter their prompt;
    Entourage owns the reusable detection, summarization, and archive policy.
    """

    def __init__(self, archive_dir: Path, utility_model: str, summary_limit: int = 3):
        self.archive_dir = Path(archive_dir)
        self.model = utility_model
        self.summary_limit = summary_limit
        self.archive_dir.mkdir(parents=True, exist_ok=True)

    def is_new_topic(self, messages: list[dict]) -> bool:
        """Return whether the last turn changes topic; model failures continue it."""
        dialogue = _dialogue_only(messages)
        if len(dialogue) < 2:
            return False
        prompt = TOPIC_JUDGE_PROMPT.format(
            history=json.dumps(dialogue[-5:-1], ensure_ascii=False, indent=2),
            last=json.dumps(dialogue[-1], ensure_ascii=False),
        )
        try:
            response = completion(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=5,
            )
            return response.choices[0].message.content.strip().upper().startswith("YES")
        except Exception as exc:
            print(f"[topic judge error: {exc}]")
            return False

    def summarize(self, messages: list[dict]) -> str:
        try:
            response = completion(
                model=self.model,
                messages=[
                    {"role": "system", "content": TOPIC_SUMMARIZER_PROMPT},
                    {
                        "role": "user",
                        "content": json.dumps(_dialogue_only(messages), ensure_ascii=False),
                    },
                ],
            )
            return response.choices[0].message.content
        except Exception as exc:
            print(f"[summarization error: {exc}]")
            return "Summary generation failed."

    def archive(self, messages: list[dict]) -> str:
        topic_id = str(uuid.uuid4())
        summary = self.summarize(messages)
        (self.archive_dir / f"summary_{topic_id}.txt").write_text(summary, encoding="utf-8")
        (self.archive_dir / f"topic_{topic_id}.json").write_text(
            json.dumps(messages, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        return topic_id

    def recent_summaries(self) -> list[str]:
        files = sorted(
            self.archive_dir.glob("summary_*.txt"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        return [path.read_text(encoding="utf-8") for path in files[: self.summary_limit]]
