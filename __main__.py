import datetime
import json
import os
import uuid
from abc import ABC, abstractmethod
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI
from tavily import TavilyClient

# --- Persona and Guidelines (Unchanged) ---
PERSONA_PROMPT = "You are Jarvis, a sophisticated and highly capable AI assistant. You are in service to a user named Aleksandr. Always be helpful, proactive, and address the user in a clear and professional manner."
GUIDELINES_PROMPT = """You have access to a set of tools to help you perform tasks and answer questions.
- Use your tools when you need to fetch external information or perform specific tasks like remembering user details.
- After using a tool, it is critical that you proceed to fully address the user's original request, synthesizing the tool's output into your final answer. Do not get distracted by the tool-use process."""


# --- New Class: ChatHistory Abstraction ---
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
            self.file_path.touch(exist_ok=True)
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


# ... (MemoryDB, Tool, TavilySearchTool, MemoryTool classes are unchanged)
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


class Tool(ABC):
    @property
    @abstractmethod
    def schema(self):
        pass

    @abstractmethod
    def execute(self, **kwargs):
        pass


class TavilySearchTool(Tool):
    def __init__(self):
        self.client = TavilyClient(api_key=os.getenv("TAVILY_API_KEY"))
        self._schema = {
            "type": "function",
            "function": {
                "name": "tavily_search",
                "description": "Get information from the web.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "The search query."}
                    },
                    "required": ["query"],
                },
            },
        }

    @property
    def schema(self):
        return self._schema

    def execute(self, query: str):
        try:
            print(f"-> Searching: '{query}'")
            return json.dumps(
                self.client.search(query, search_depth="basic")["results"]
            )
        except Exception as e:
            return f"Error: {e}"


class MemoryTool(Tool):
    def __init__(self, memory_db: MemoryDB):
        self.memory_db = memory_db
        self._schema = {
            "type": "function",
            "function": {
                "name": "save_user_memory",
                "description": "Use to remember a fact about the user. Then, continue with the original request.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "fact_to_remember": {
                            "type": "string",
                            "description": "A concise fact to remember.",
                        }
                    },
                    "required": ["fact_to_remember"],
                },
            },
        }

    @property
    def schema(self):
        return self._schema

    def execute(self, fact_to_remember: str):
        print(f"-> Remembering: '{fact_to_remember}'")
        self.memory_db.add(fact_to_remember)
        return f"Success: Fact saved."


class Agent:
    # --- Modified: Agent now uses ChatHistory ---
    def __init__(self, model, history: ChatHistory, tools_list: list[Tool] = None):
        self.model = model
        self.client = OpenAI()
        self.history = history  # The agent now holds a reference to the history object

        # Tool setup remains the same
        self.tools_schemas = []
        self.available_tools = {}
        if tools_list:
            for tool in tools_list:
                self.tools_schemas.append(tool.schema)
                tool_name = tool.schema["function"]["name"]
                self.available_tools[tool_name] = tool.execute

    def __call__(self, prompt):
        # The agent no longer adds the system prompt; it's assumed to be in the history
        user_message = {"role": "user", "content": prompt}
        self.history.append(user_message)

        while True:
            # Get messages from the history object, stripping our internal timestamp for the API
            api_messages = [
                {k: v for k, v in m.items() if k != "timestamp"}
                for m in self.history.get_messages()
            ]

            response = self.client.chat.completions.create(
                model=self.model,
                messages=api_messages,
                tools=self.tools_schemas if self.tools_schemas else None,
                tool_choice="auto",
            )
            response_message = response.choices[0].message

            if not response_message.tool_calls:
                self.history.append(response_message.model_dump())
                return response_message.content

            print("-> Model requested tool call(s)...")
            self.history.append(response_message.model_dump())

            for tool_call in response_message.tool_calls:
                function_name = tool_call.function.name
                function_to_call = self.available_tools.get(
                    function_name, lambda: f"Unknown tool: {function_name}"
                )

                function_args = json.loads(tool_call.function.arguments)
                function_response = function_to_call(**function_args)

                tool_message = {
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "name": function_name,
                    "content": function_response,
                }
                self.history.append(tool_message)


class CLI:
    # --- Modified: CLI now uses ChatHistory ---
    def __init__(self):
        self.chat_dir = Path(os.path.expanduser("~/.entourage/chats"))
        self.chat_dir.mkdir(parents=True, exist_ok=True)
        memory_path = Path(os.path.expanduser("~/.entourage/memory.txt"))
        self.memory_db = MemoryDB(memory_path)

        self.history: ChatHistory | None = None
        self.agent: Agent | None = None

        self._load_latest_chat()

    def _load_latest_chat(self):
        """Finds the latest chat and initializes history and agent for it."""
        chat_files = list(self.chat_dir.glob("*.json"))
        if chat_files:
            latest_file = max(chat_files, key=lambda f: f.stat().st_mtime)
            chat_id = latest_file.stem
            self.history = ChatHistory(chat_id, self.chat_dir)
        else:
            # No chats exist, so start a new one
            self._new_chat()
            return  # _new_chat handles agent creation

        self._create_agent()
        self._print_history()

    def _create_agent(self):
        """Instantiates the agent, using the current self.history object."""
        # The system prompt is now managed within the history, so we re-apply it
        # if the loaded history doesn't start with one.
        current_messages = self.history.get_messages()
        if not current_messages or current_messages[0].get("role") != "system":
            # Build the dynamic prompt
            memories = self.memory_db.get_all()
            memory_prompt_part = ""
            if memories:
                facts = "\n".join(
                    f"- {fact.split('] ')[1]}" for fact in memories if "] " in fact
                )
                memory_prompt_part = (
                    "\n\nHere are facts you remember about Aleksandr:\n" + facts
                )
            final_system_prompt = (
                f"{PERSONA_PROMPT}\n\n{GUIDELINES_PROMPT}{memory_prompt_part}".strip()
            )

            # Prepend system prompt to existing messages
            updated_messages = [
                {"role": "system", "content": final_system_prompt}
            ] + current_messages
            self.history.set_messages(updated_messages)

        tavily_tool = TavilySearchTool()
        memory_tool = MemoryTool(self.memory_db)

        self.agent = Agent(
            model="gpt-4o",
            history=self.history,  # Pass the history object to the agent
            tools_list=[tavily_tool, memory_tool],
        )

    def _new_chat(self):
        """Creates a new history object and a corresponding agent."""
        new_chat_id = str(uuid.uuid4())
        self.history = ChatHistory(new_chat_id, self.chat_dir)
        self._create_agent()  # This will create the agent and add the system prompt
        print(f"\n[New chat started as Jarvis. Chat ID: {new_chat_id}]\n")

    def _print_history(self):
        messages = self.history.get_messages()
        # This method remains largely the same, but the printout is improved
        if not messages or all(m.get("role") == "system" for m in messages):
            print("[No previous chat history found.]\n")
            return
        print("[Loaded previous chat history:]\n")
        for msg in messages:
            role = msg.get("role", "unknown").capitalize()
            content = msg.get("content", "")
            timestamp = msg.get("timestamp", "")

            if role == "User":
                print(f"You ({timestamp}):\n{content}\n")
            elif role == "Assistant":
                if msg.get("tool_calls"):
                    tool_name = msg["tool_calls"][0]["function"]["name"]
                    args = msg["tool_calls"][0]["function"]["arguments"]
                    print(
                        f"Assistant ({timestamp}):\n[Requested to use tool: {tool_name} with args: {args}]\n"
                    )
                elif content:
                    print(f"Assistant ({timestamp}):\n{content}\n")
            elif role == "Tool":
                # Optionally make this more verbose if you want to see tool output in history
                print(f"Tool ({timestamp}):\n[Executed {msg.get('name')}]\n")
            elif role == "System":
                pass  # Don't print the system prompt on load

        print("-" * 40 + "\n")

    def run(self):
        while True:
            print("> ", end="")
            try:
                user_message = input()
            except EOFError:
                break
            if not user_message:
                break

            if user_message.strip() == "/new":
                self._new_chat()
                continue

            res = self.agent(user_message)
            print(f"\nJarvis:\n{res}\n")


load_dotenv()
cli = CLI()
cli.run()
