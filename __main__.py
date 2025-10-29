import asyncio
import datetime
import json
import os
import uuid
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv
from fastmcp import Client as MCPClient
from openai import OpenAI
from tavily import TavilyClient

from utils import pprint


class PersonaConfig:
    """A single place to configure the assistant's persona."""

    def __init__(
        self, agent_name: str, user_name: str, persona_template: str, guidelines: str
    ):
        self.agent_name = agent_name
        self.user_name = user_name
        self.persona_template = persona_template
        self.guidelines = guidelines


guidelines = """
You are a conversational AI. Follow all instructions below precisely.

---
### CORE INSTRUCTIONS [EN]

- **Primary Goal:** Act as a helpful and wise conversational partner based on the persona defined below.
- **CRITICAL RULE:** You must NEVER break character.
- **Safety:** Decline any harmful or inappropriate requests.
- You have access to a set of tools to help you perform tasks and answer questions.
- Use your tools when you need to fetch external information or perform specific tasks like remembering user details.
- After using a tool, it is critical that you proceed to fully address the user's original request, synthesizing the tool's output into your final answer. Do not get distracted by the tool-use process.
"""

# name = "ゆりこ"
# persona = """
# ### PERSONA & STYLE [JA]

# # あなたの人格 (Your Persona)
# - あなたは、森の奥深くに住む、歳を重ねた賢い狐の化身です。
# - 人間に対しては友好的ですが、少し古風で神秘的な話し方をします。
# - 一人称は「わし」を使い、語尾には「〜じゃ」「〜のう」「〜じゃよ」などをよく使います。丁寧語（です・ます）は使いません。

# # 話し方の例 (Speech Examples)
# - 「ほう、面白いことを聞くのう。それはじゃな…」
# - 「わしが知る限り、その答えは…じゃよ。」
# - 「ふむ。人間の考えることは、いつの時代も興味深いものじゃ。」
# """
name = "Jarvis"
persona = "You are Jarvis, a helpful assistant to Sasha."
APP_CONFIG = PersonaConfig(
    agent_name=name,
    user_name="Aleksandr",
    persona_template=persona,
    guidelines=guidelines,
)


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
    async def execute(self, **kwargs):
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

    async def execute(self, query: str):
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

    async def execute(self, fact_to_remember: str):
        print(f"-> Remembering: '{fact_to_remember}'")
        self.memory_db.add(fact_to_remember)
        return f"Success: Fact saved."


class MCPTool(Tool):
    def __init__(self, client, proxy):
        self._proxy = proxy
        self.client = client
        self._schema = {
            "type": "function",
            "function": {
                "name": proxy.name,
                "description": proxy.description,
                "parameters": proxy.inputSchema,
            },
        }

    @property
    def schema(self):
        return self._schema

    async def execute(self, **kwargs):
        result = await self.client.call_tool(self._proxy.name, kwargs)
        # print("MCP returned:", result)
        return result.content[0].text


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

    async def __call__(self, prompt):
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
                print("TOOL:", function_name)
                print("ARGS:", function_args)
                function_response = await function_to_call(**function_args)
                # print(function_response)
                tool_message = {
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "name": function_name,
                    "content": function_response,
                }
                self.history.append(tool_message)


class CLI:
    def __init__(self, config: PersonaConfig):
        self.config = config
        agent_name_lower = self.config.agent_name.lower()

        agent_base_dir = Path(os.path.expanduser(f"~/.entourage/{agent_name_lower}"))
        agent_base_dir.mkdir(parents=True, exist_ok=True)

        self.chat_dir = agent_base_dir / "chats"
        self.chat_dir.mkdir(parents=True, exist_ok=True)

        memory_path = agent_base_dir / "memory.txt"
        self.memory_db = MemoryDB(memory_path)

        self.history: ChatHistory | None = None
        self.agent: Agent | None = None

        # self._load_latest_chat()
        self._new_chat()
        self._print_history()

    async def setup(self):
        mcp_config = {
            "mcpServers": {"fetch": {"command": "uvx", "args": ["mcp-server-fetch"]}}
        }
        obj = MCPClient(mcp_config)
        self.mcp_client = await obj.__aenter__()
        await self._create_agent()

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

    async def _create_agent(self):
        # --- Modified: Prompts are now built from the config object ---
        current_messages = self.history.get_messages()
        if not current_messages or current_messages[0].get("role") != "system":
            # Part 1: Persona
            persona_prompt = self.config.persona_template.format(
                agent_name=self.config.agent_name, user_name=self.config.user_name
            )
            # Part 2: Guidelines
            guidelines_prompt = self.config.guidelines
            # Part 3: Memory
            memories = self.memory_db.get_all()
            memory_prompt_part = ""
            if memories:
                facts = "\n".join(
                    f"- {fact.split('] ')[1]}" for fact in memories if "] " in fact
                )
                # No more hard-coded name!
                memory_prompt_part = (
                    f"\n\nHere are facts you remember about {self.config.user_name}:\n"
                    + facts
                )

            final_system_prompt = (
                f"{persona_prompt}\n\n{guidelines_prompt}{memory_prompt_part}".strip()
            )

            updated_messages = [
                {"role": "system", "content": final_system_prompt}
            ] + current_messages
            self.history.set_messages(updated_messages)

        tavily_tool = TavilySearchTool()
        memory_tool = MemoryTool(self.memory_db)

        # mcp_server_url = "http://localhost:8000/mcp"
        mcp_tools = await self.get_mcp_tools()
        all_tools = [tavily_tool, memory_tool] + mcp_tools

        self.agent = Agent(
            # model="gpt-5-chat-latest",
            model="gpt-5-mini-2025-08-07",
            history=self.history,
            tools_list=all_tools,
        )

    def _new_chat(self):
        """Creates a new history object and a corresponding agent."""
        new_chat_id = str(uuid.uuid4())
        self.history = ChatHistory(new_chat_id, self.chat_dir)
        self._create_agent()  # This will create the agent and add the system prompt
        print(f"\n[New chat started. Chat ID: {new_chat_id}]\n")

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

    async def get_mcp_tools(self):
        """Connects to an MCP server and fetches its available tools."""
        try:
            # print(f"-> Connecting to MCP server at {server_url}...")
            mcp_tools = await self.mcp_client.list_tools()
            # tool_names = [t.schema["function"]["name"] for t in mcp_tools]
            tool_names = [t.name for t in mcp_tools]
            print(
                f"-> Successfully loaded {len(mcp_tools)} tools: {', '.join(tool_names)}"
            )
            tools = [MCPTool(self.mcp_client, t) for t in mcp_tools]
            return tools
        except requests.exceptions.RequestException as e:
            print(
                f"Warning: Could not connect to MCP server at {server_url}. Error: {e}"
            )
            return []
        except Exception as e:
            print(
                f"Warning: An unexpected error occurred while fetching MCP tools: {e}"
            )
            return []

    async def run(self):
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

            res = await self.agent(user_message)
            print(f"\n{name}:\n{res}\n")


async def main():
    load_dotenv()
    cli = CLI(config=APP_CONFIG)
    await cli.setup()
    await cli.run()


asyncio.run(main())
