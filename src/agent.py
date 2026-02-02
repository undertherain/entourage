from openai import OpenAI

from .flow import Sequence
from .utils import pprint


class AgentWithTools:
    def __init__(self, model, tools):
        self.model = model
        self.tools = tools
        self.client = OpenAI()
        self.tools_schemas = []
        self.available_tools = {}
        if tools:
            for tool in tools:
                self.tools_schemas.append(tool.schema)
                tool_name = tool.schema["function"]["name"]
                self.available_tools[tool_name] = tool.execute

    def __call__(self, state):

        print("Agent with tools, got state:")
        pprint(state)
        # if "tool_call_result" in state:
        #   print("got fake tool result, exiting")
        #  return state, None
        response = self.client.chat.completions.create(
            model=self.model,
            messages=state["messages"],
            tools=self.tools_schemas,
            # tools=self.tools_schemas if self.tools_schemas else None,
            # tool_choice="auto",
        )  # expect messages in state["messages"]
        response_message = response.choices[0].message
        state["messages"].append(response_message.model_dump())
        # if last response is user message: call llm
        # if last response is tool call: call all tools amd loop back to itself
        # dummy_tool_invocation = {
        #     "role": "assistant",
        #     "tool_calls": "call some tools",
        # }
        if not response_message.tool_calls:
            print("agent done, returning state:")
            # pprint(state)
            print(state["messages"][-1]["content"])
            return state, None
        print("scheduling a tool")
        print(response_message)
        # state["messages"].append(dummy_tool_invocation)
        # for now let's assyme a single tool call. let's explicitly assert it.
        # print("MESSAGES:", len(state["messages"]))
        # if len(state["messages"]) < 3:
        return (state, Sequence(self.tools[0], self))
        # print("simulating tool is done")
        # if last response is tool result: call LLM again

    @property
    def __name__(self):
        return self.__class__.__name__
