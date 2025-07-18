import datetime
import json
import os
import uuid
from pathlib import Path

from dotenv import load_dotenv
from openai import OpenAI
from tavily import TavilyClient

# Define a system prompt to guide the agent
SYSTEM_PROMPT = """You are a helpful assistant. You have access to a web search tool called 'tavily_search'.
Use this tool to answer questions that require up-to-date information, current events, or specific details you might not know.
When you use the search tool, present the search results to the user in a clear and concise way before answering the final question.
"""


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
        if system_prompt:
            # --- New/Modified ---: Prepend the system message
            self.messages.append({"role": "system", "content": system_prompt})

        self.chat_file_path = chat_file_path

        # --- New/Modified ---: Initialize Tavily client and define tools
        self.tavily_client = TavilyClient(api_key=os.getenv("TAVILY_API_KEY"))
        self.tools = [
            {
                "type": "function",
                "function": {
                    "name": "tavily_search",
                    "description": "Get information from the web using Tavily search API. Use this for questions about current events, up-to-date information, or topics you are not trained on.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "The search query to use. For example: 'current stock price of AAPL'",
                            }
                        },
                        "required": ["query"],
                    },
                },
            }
        ]
        self.available_tools = {
            "tavily_search": self.tavily_search,
        }

    # --- New/Modified ---: The actual function that performs the web search
    def tavily_search(self, query: str):
        """Performs a web search using the Tavily client."""
        try:
            print(f"-> Searching the web for: '{query}'")
            response = self.tavily_client.search(query, search_depth="basic")
            # We'll return a formatted string of the results for the model
            return json.dumps(response["results"])
        except Exception as e:
            return f"Error performing search: {e}"

    def append_and_save(self, message):
        # --- New/Modified ---: Simplified to take the whole message dictionary
        message_with_timestamp = {
            **message,
            "timestamp": datetime.datetime.now().isoformat(),
        }
        self.messages.append(message_with_timestamp)
        if self.chat_file_path:
            try:
                # Filter out timestamp for OpenAI API compatibility if needed elsewhere
                messages_to_save = [{k: v for k, v in m.items()} for m in self.messages]
                with open(self.chat_file_path, "w", encoding="utf-8") as f:
                    json.dump(messages_to_save, f, ensure_ascii=False, indent=2)
            except Exception as e:
                print(f"Warning: Failed to save chat to {self.chat_file_path}: {e}")

    def __call__(self, prompt):
        # --- New/Modified ---: Append user message as a dictionary
        user_message = {"role": "user", "content": prompt}
        self.append_and_save(user_message)

        while True:
            # --- New/Modified ---: Prepare messages for API call (remove our custom timestamp)
            api_messages = [
                {
                    "role": m["role"],
                    "content": m["content"],
                    **({"tool_calls": m["tool_calls"]} if "tool_calls" in m else {}),
                    **(
                        {"tool_call_id": m["tool_call_id"]}
                        if "tool_call_id" in m
                        else {}
                    ),
                }
                for m in self.messages
                if m.get("role") != "system"
                or m.get("content")  # include system prompts
            ]

            # The system prompt is now managed in self.messages
            system_message = next(
                (m for m in self.messages if m["role"] == "system"), None
            )
            final_api_messages = (
                [system_message] if system_message else []
            ) + api_messages

            response = self.client.chat.completions.create(
                model=self.model,
                messages=final_api_messages,
                tools=self.tools,
                tool_choice="auto",
            )
            response_message = response.choices[0].message

            # --- New/Modified ---: Check if the model wants to use a tool
            if not response_message.tool_calls:
                # No tool call, it's a final answer
                self.append_and_save(response_message.model_dump())
                return response_message.content

            # --- New/Modified ---: Process tool calls
            print("-> Model requested tool call(s)...")
            self.append_and_save(
                response_message.model_dump()
            )  # Save the assistant's decision to call a tool

            for tool_call in response_message.tool_calls:
                function_name = tool_call.function.name
                function_to_call = self.available_tools.get(function_name)

                if not function_to_call:
                    print(
                        f"Error: Model tried to call unknown function '{function_name}'"
                    )
                    continue

                function_args = json.loads(tool_call.function.arguments)
                function_response = function_to_call(**function_args)

                # --- New/Modified ---: Append the tool's response to the history
                tool_message = {
                    "role": "tool",
                    "tool_call_id": tool_call.id,
                    "name": function_name,
                    "content": function_response,
                }
                self.append_and_save(tool_message)

            # Loop back to call the model again with the tool response included


class CLI:
    def __init__(self):
        chat_dir = Path(os.path.expanduser("~/.entourage/chats"))
        chat_dir.mkdir(parents=True, exist_ok=True)
        chat_files = list(chat_dir.glob("*.json"))
        if chat_files:
            # Find the most recently modified chat file
            latest_file = max(chat_files, key=lambda f: f.stat().st_mtime)
            chat_file_path = latest_file
            # Load messages from the latest file
            try:
                with open(chat_file_path, "r", encoding="utf-8") as f:
                    messages = json.load(f)
            except Exception as e:
                print(f"Warning: Failed to load chat from {chat_file_path}: {e}")
                messages = []
            self.agent = Agent(model="gpt-4.1-nano", chat_file_path=str(chat_file_path))
            self.agent.messages = messages
            self._print_history(messages)
        else:
            # No chat files exist, create a new one
            chat_uuid = str(uuid.uuid4())
            chat_file_path = chat_dir / f"{chat_uuid}.json"
            self.agent = Agent(model="gpt-4.1-nano", chat_file_path=str(chat_file_path))

    def _print_history(self, messages):
        if not messages:
            print("[No previous chat history found.]\n")
            return
        print("[Loaded previous chat history:]\n")
        for msg in messages:
            role = msg.get("role", "unknown").capitalize()
            content = msg.get("content", "")
            timestamp = msg.get("timestamp", "")
            print(f"{role} ({timestamp}):\n{content}\n")
        print("-" * 40 + "\n")

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
