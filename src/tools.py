import json
import os
from abc import ABC, abstractmethod

from tavily import TavilyClient

from .utils import pprint


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
        print("CALLING TOOL STUB")
        # print("got state", state)
        # for now assume single tool call and that it is in the state
        tool_params = state["messages"][-1]["tool_calls"][0]
        print("calling tool as ", tool_params)
        tool_result = self.execute(**json.loads(tool_params["function"]["arguments"]))
        print("tool returned")
        pprint(tool_result)
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
            print(f"-> Searching: '{query}'")
            return json.dumps(
                self.client.search(query, search_depth="basic")["results"]
            )
        except Exception as e:
            return f"Error: {e}"
