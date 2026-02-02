import json
import datetime
from pathlib import Path

class ChatHistory:
    """Manages loading, saving, and accessing conversation messages for a single chat session."""

    def __init__(self, chat_id: str, history_dir: Path):
        self.chat_id = chat_id
        self.history_dir = history_dir
        self.file_path = self.history_dir / f"{self.chat_id}.json"
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
