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
