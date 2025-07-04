import datetime
import json
import os
import uuid
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI


class Agent:
    def __init__(
        self,
        model,
        system_prompt=None,
        chat_file_path=None,
    ):
        self.model = model
        self.client = OpenAI()
        self.messages = []
        self.chat_file_path = chat_file_path

    def append_and_save(self, role, content):
        message = {
            "role": role,
            "content": content,
            "timestamp": datetime.datetime.now().isoformat(),
        }
        self.messages.append(message)
        if self.chat_file_path:
            try:
                with open(self.chat_file_path, "w", encoding="utf-8") as f:
                    json.dump(self.messages, f, ensure_ascii=False, indent=2)
            except Exception as e:
                print(f"Warning: Failed to save chat to {self.chat_file_path}: {e}")

    def __call__(self, prompt):
        self.append_and_save("user", prompt)
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": m["role"], "content": m["content"]} for m in self.messages
            ],
            # tools=tools,
            # tool_choice="auto",  # auto is default, but we'll be explicit
        )
        # TODO: tream multiple choices
        response_message = response.choices[0].message
        self.append_and_save("assistant", response_message.content)
        return response_message.content


class CLI:
    def __init__(self):
        # Generate UUID for this chat session
        chat_uuid = str(uuid.uuid4())
        # Ensure ~/.entourage/chats exists
        chat_dir = Path(os.path.expanduser("~/.entourage/chats"))
        chat_dir.mkdir(parents=True, exist_ok=True)
        # Set chat file path
        chat_file_path = chat_dir / f"{chat_uuid}.json"
        self.agent = Agent(model="gpt-4.1-nano", chat_file_path=str(chat_file_path))

    def run(self):
        while True:
            print("> ", end="")
            try:
                user_message = input()
            except EOFError as e:
                break
            if not user_message:
                break

            if user_message.strip() == "/new":
                # Reset chat history and create new chat file
                self.agent.messages = []
                new_chat_uuid = str(uuid.uuid4())
                chat_dir = Path(os.path.expanduser("~/.entourage/chats"))
                chat_dir.mkdir(parents=True, exist_ok=True)
                new_chat_file_path = chat_dir / f"{new_chat_uuid}.json"
                self.agent.chat_file_path = str(new_chat_file_path)
                print(f"\n[New chat started. Chat ID: {new_chat_uuid}]\n")
                continue

            res = self.agent.__call__(user_message)
            print()
            print(res)
            print()


load_dotenv()
cli = CLI()
cli.run()
# agent = Agent()
# agent()
# res = agent("whats's two plus two?")
# print(res)
