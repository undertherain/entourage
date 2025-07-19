import datetime
import json
import os
import uuid
from abc import ABC, abstractmethod
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI
from tavily import TavilyClient

PERSONA_PROMPT = "You are Jarvis, a sophisticated and highly capable AI assistant. You are in service to a user named Aleksandr. Always be helpful, proactive, and address the user in a clear and professional manner."

# --- New/Modified ---
# Part 2: The Core Behavioral Guidelines
GUIDELINES_PROMPT = """You have access to a set of tools to help you perform tasks and answer questions.
- Use your tools when you need to fetch external information, perform calculations, or remember user details.
- After using a tool, it is critical that you proceed to fully address the user's original request, synthesizing the tool's output into your final answer. Do not get distracted by the tool-use process."""


class MemoryDB:
    """A simple text-file based database for storing facts."""

    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        self.file_path.touch(exist_ok=True)  # Ensure the file exists

    def add(self, fact: str):
        """Adds a fact to the database with a timestamp."""
        timestamp = datetime.datetime.now().isoformat()
        with open(self.file_path, "a", encoding="utf-8") as f:
            f.write(f"[{timestamp}] {fact}\n")

    def get_all(self) -> list[str]:
        """Retrieves all facts from the database."""
        with open(self.file_path, "r", encoding="utf-8") as f:
            return [line.strip() for line in f.readlines()]


class Tool(ABC):
    """Abstract base class for a tool that the agent can use."""

    @property
    @abstractmethod
    def schema(self):
        """The JSON schema for the tool, as required by OpenAI."""
        pass

    @abstractmethod
    def execute(self, **kwargs):
        """Executes the tool's logic."""
        pass


class MemoryTool(Tool):
    """A tool for remembering specific facts about the user."""

    def __init__(self, memory_db: MemoryDB):
        self.memory_db = memory_db
        self._schema = {
            "type": "function",
            "function": {
                "name": "save_user_memory",
                "description": (
                    "Use this tool to remember a specific, succinct fact about the user or their preferences. "
                    "If the user states a personal detail, preference, or something they ask you to remember, "
                    "formulate it as a concise statement and save it. Then, continue with the user's original request."
                ),
                "parameters": {
                    "type": "object",
                    "properties": {
                        "fact_to_remember": {
                            "type": "string",
                            "description": "A single, concise fact to remember about the user (e.g., 'The user's favorite color is green.')",
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
        """Saves the fact to the memory database."""
        print(f"-> Remembering fact: '{fact_to_remember}'")
        self.memory_db.add(fact_to_remember)
        return f"Success: The fact '{fact_to_remember}' has been saved."


class TavilySearchTool(Tool):
    """A tool for performing web searches using the Tavily API."""

    def __init__(self):
        self.client = TavilyClient(api_key=os.getenv("TAVILY_API_KEY"))
        self._schema = {
            "type": "function",
            "function": {
                "name": "tavily_search",
                "description": "Get information from the web using Tavily search API. Use this for questions about current events, up-to-date information, or topics you are not trained on.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "The search query to use. For example: 'What is the weather in Tokyo?'",
                        }
                    },
                    "required": ["query"],
                },
            },
        }

    @property
    def schema(self):
        return self._schema

    def execute(self, query: str):
        """Performs a web search and returns the results."""
        try:
            print(f"-> Searching the web for: '{query}'")
            response = self.client.search(query, search_depth="basic")
            return json.dumps(response["results"])
        except Exception as e:
            return f"Error performing search: {e}"


class Agent:
    def __init__(
        self,
        model,
        system_prompt=None,
        chat_file_path=None,
        tools_list: list[Tool] = None,  # --- New/Modified ---
    ):
        self.model = model
        self.client = OpenAI()
        self.messages = []
        if system_prompt:
            self.messages.append({"role": "system", "content": system_prompt})

        self.chat_file_path = chat_file_path

        # --- New/Modified ---: Configure tools from the provided list
        self.tools_schemas = []
        self.available_tools = {}
        if tools_list:
            for tool in tools_list:
                self.tools_schemas.append(tool.schema)
                tool_name = tool.schema["function"]["name"]
                self.available_tools[tool_name] = tool.execute

    def append_and_save(self, message):
        message_with_timestamp = {
            **message,
            "timestamp": datetime.datetime.now().isoformat(),
        }
        self.messages.append(message_with_timestamp)
        if self.chat_file_path:
            try:
                messages_to_save = [m for m in self.messages]
                with open(self.chat_file_path, "w", encoding="utf-8") as f:
                    json.dump(messages_to_save, f, ensure_ascii=False, indent=2)
            except Exception as e:
                print(f"Warning: Failed to save chat to {self.chat_file_path}: {e}")

    def __call__(self, prompt):
        user_message = {"role": "user", "content": prompt}
        self.append_and_save(user_message)

        while True:
            # Prepare messages for the API, removing our internal timestamp
            api_messages = [
                {k: v for k, v in m.items() if k != "timestamp"} for m in self.messages
            ]

            response = self.client.chat.completions.create(
                model=self.model,
                messages=api_messages,
                tools=(
                    self.tools_schemas if self.tools_schemas else None
                ),  # Pass schemas to OpenAI
                tool_choice="auto",
            )
            response_message = response.choices[0].message

            if not response_message.tool_calls:
                self.append_and_save(response_message.model_dump())
                return response_message.content

            print("-> Model requested tool call(s)...")
            self.append_and_save(response_message.model_dump())

            for tool_call in response_message.tool_calls:
                function_name = tool_call.function.name
                function_to_call = self.available_tools.get(function_name)

                if not function_to_call:
                    # In a real app, you might want to handle this more gracefully
                    raise ValueError(
                        f"Model tried to call unknown function '{function_name}'"
                    )

                function_args = json.loads(tool_call.function.arguments)
                function_response = function_to_call(**function_args)

                tool_message = {
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "name": function_name,
                    "content": function_response,
                }
                self.append_and_save(tool_message)


class CLI:
    def __init__(self):
        chat_dir = Path(os.path.expanduser("~/.entourage/chats"))
        chat_dir.mkdir(parents=True, exist_ok=True)
        chat_files = list(chat_dir.glob("*.json"))
        memory_path = Path(os.path.expanduser("~/.entourage/memory.txt"))
        self.memory_db = MemoryDB(memory_path)

        self.agent = None

        if chat_files:
            latest_file = max(chat_files, key=lambda f: f.stat().st_mtime)
            chat_file_path = latest_file
            try:
                with open(chat_file_path, "r", encoding="utf-8") as f:
                    messages = json.load(f)

                self._create_agent(str(chat_file_path))  # --- Refactored agent creation
                self.agent.messages = messages
                self._print_history(messages)
            except Exception as e:
                print(f"Warning: Failed to load chat from {chat_file_path}: {e}")
                self._new_chat()
        else:
            self._new_chat()

    def _create_agent(self, chat_file_path: str):
        """Instantiates tools and the agent."""
        # --- New/Modified ---: Instantiate and pass tools to the agent
        memories = self.memory_db.get_all()
        memory_prompt_part = ""
        if memories:
            facts = "\n".join(
                f"- {fact.split('] ')[1]}" for fact in memories if "] " in fact
            )
            memory_prompt_part = (
                "Finally, you have remembered the following facts about Aleksandr. "
                "Use them to personalize your responses and demonstrate your memory:\n"
                + facts
            )

        # Combine the modular parts into a final system prompt
        final_system_prompt = (
            f"{PERSONA_PROMPT}\n\n{GUIDELINES_PROMPT}\n\n{memory_prompt_part}".strip()
        )

        tavily_tool = TavilySearchTool()
        memory_tool = MemoryTool(self.memory_db)

        self.agent = Agent(
            model="gpt-4o",
            chat_file_path=chat_file_path,
            system_prompt=final_system_prompt,
            tools_list=[tavily_tool, memory_tool],  # Pass the list of tool objects
        )

    def _new_chat(self):
        """Creates a new agent and chat file."""
        chat_dir = Path(os.path.expanduser("~/.entourage/chats"))
        chat_uuid = str(uuid.uuid4())
        chat_file_path = chat_dir / f"{chat_uuid}.json"
        self._create_agent(str(chat_file_path))
        print(f"\n[New chat started. Chat ID: {chat_uuid}]\n")

    def _print_history(self, messages):
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
            print()
            print(res)
            print()


load_dotenv()
cli = CLI()
cli.run()
