import json
import os
import logging
from abc import ABC, abstractmethod

from tavily import TavilyClient

from .utils import pprint

logger = logging.getLogger(__name__)


class Tool(ABC):
    @property
    @abstractmethod
    def schema(self):
        pass

    @abstractmethod
    def execute(self, **kwargs):
        pass

    @property
    def __name__(self):
        return self.__class__.__name__

    def __call__(self, state):
        logger.debug("CALLING TOOL STUB")
        # logger.debug("got state %s", state)
        # for now assume single tool call and that it is in the state
        tool_params = state["messages"][-1]["tool_calls"][0]
        logger.debug("calling tool as %s", tool_params)
        print(f"[System] Calling tool: {tool_params['function']['name']}")
        tool_result = self.execute(**json.loads(tool_params["function"]["arguments"]))
        logger.debug("tool returned")
        # pprint(tool_result) # avoiding pprint in logs
        logger.debug(tool_result)
        tool_message = {
            "role": "tool",
            "tool_call_id": tool_params["id"],
            "name": tool_params["function"]["name"],
            "content": tool_result,
        }
        state["messages"].append(tool_message)
        return state, None


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
            logger.info(f"-> Searching: '{query}'")
            return json.dumps(
                self.client.search(query, search_depth="basic")["results"]
            )
        except Exception as e:
            return f"Error: {e}"


class MemoryTool(Tool):
    def __init__(self, memory_db):
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
        logger.info(f"-> Remembering: '{fact_to_remember}'")
        self.memory_db.add(fact_to_remember)
        return f"Success: Fact saved."


class ListDirTool(Tool):
    def __init__(self):
        self._schema = {
            "type": "function",
            "function": {
                "name": "list_files",
                "description": "List files and directories in a given path. Useful for exploring the project structure.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "The path to list. Defaults to current directory '.'",
                        }
                    },
                    "required": ["path"],
                },
            },
        }

    @property
    def schema(self):
        return self._schema

    def execute(self, path: str = "."):
        try:
            expanded_path = os.path.expanduser(path)
            if not os.path.exists(expanded_path):
                return f"Error: Path '{path}' does not exist."
            
            items = os.listdir(expanded_path)
            # Add type info (file or dir)
            result = []
            for item in items:
                 full_path = os.path.join(expanded_path, item)
                 type_str = "DIR" if os.path.isdir(full_path) else "FILE"
                 result.append(f"[{type_str}] {item}")
            
            return "\n".join(sorted(result))
        except Exception as e:
            return f"Error: {e}"


class ReadFileTool(Tool):
    def __init__(self):
        self._schema = {
            "type": "function",
            "function": {
                "name": "read_file",
                "description": "Read the content of a file.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "The path to the file to read.",
                        }
                    },
                    "required": ["path"],
                },
            },
        }

    @property
    def schema(self):
        return self._schema

    def execute(self, path: str):
        try:
            expanded_path = os.path.expanduser(path)
            if not os.path.exists(expanded_path):
                 return f"Error: File '{path}' does not exist."
            
            if not os.path.isfile(expanded_path):
                 return f"Error: '{path}' is not a file."

            with open(expanded_path, "r", encoding="utf-8") as f:
                content = f.read()
                
            return content
        except Exception as e:
            return f"Error reading file: {e}"


class WriteFileTool(Tool):
    def __init__(self):
        self._schema = {
            "type": "function",
            "function": {
                "name": "write_file",
                "description": "Write content to a file. Behavior: overwritten if exists, created if not.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": {
                            "type": "string",
                            "description": "The path to the file to write.",
                        },
                        "content": {
                            "type": "string",
                            "description": "The full content to write to the file.",
                        }
                    },
                    "required": ["path", "content"],
                },
            },
        }

    @property
    def schema(self):
        return self._schema

    def execute(self, path: str, content: str):
        try:
            expanded_path = os.path.expanduser(path)
            
            # Ensure parent directory exists
            parent_dir = os.path.dirname(expanded_path)
            if parent_dir and not os.path.exists(parent_dir):
                os.makedirs(parent_dir)
            
            with open(expanded_path, "w", encoding="utf-8") as f:
                f.write(content)
                
            return f"Success: File '{path}' written."
        except Exception as e:
            return f"Error writing file: {e}"
