from litellm import completion
import warnings

# Suppress Pydantic serialization warnings globally
warnings.filterwarnings("ignore", category=UserWarning, module="pydantic")

from .flow import Sequence
from .utils import pprint


class AgentWithTools:
    def __init__(self, model, tools, base_url=None, debug=False):
        self.model = model
        self.tools = tools
        self.debug = debug
        self.base_url = base_url
        self.tools_schemas = []
        self.available_tools = {}
        if tools:
            for tool in tools:
                self.tools_schemas.append(tool.schema)
                tool_name = tool.schema["function"]["name"]
                self.available_tools[tool_name] = tool.execute

    def __call__(self, state):

        if self.debug:
            print("Agent with tools, got state:")
            pprint(state)
        # if "tool_call_result" in state:
        #   print("got fake tool result, exiting")
        #  return state, None
        try:
            kwargs = {
                "model": self.model,
                "messages": state["messages"],
                "tools": self.tools_schemas,
            }
            if self.base_url:
                kwargs["base_url"] = self.base_url
                
            import warnings
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning, module="pydantic")
                response = completion(**kwargs)  # expect messages in state["messages"]
        except Exception as e:
            print(f"Error calling LLM: {e}")
            return state, None

        response_message = response.choices[0].message
        
        # Handle potential differences in litellm response types
        # Manually construct dict to avoid Pydantic validation warnings/errors with litellm types
        msg_dict = {
            "role": response_message.role,
            "content": response_message.content,
        }
        if response_message.tool_calls:
            msg_dict["tool_calls"] = []
            for tc in response_message.tool_calls:
                # litellm tool calls might be Objects or dicts
                if hasattr(tc, 'model_dump'):
                    tc_dict = tc.model_dump()
                else:
                    tc_dict = dict(tc)
                msg_dict["tool_calls"].append(tc_dict)
                
        if hasattr(response_message, 'function_call') and response_message.function_call:
             if hasattr(response_message.function_call, 'model_dump'):
                 msg_dict["function_call"] = response_message.function_call.model_dump()
             else:
                 msg_dict["function_call"] = dict(response_message.function_call)

        state["messages"].append(msg_dict)
        # if last response is user message: call llm
        # if last response is tool call: call all tools amd loop back to itself
        # dummy_tool_invocation = {
        #     "role": "assistant",
        #     "tool_calls": "call some tools",
        # }
        if not response_message.tool_calls:
            if self.debug:
                print("agent done, returning state:")
                
            content = state['messages'][-1].get('content', '')
            print(f"assistant: {content}")
            return state, None
        
        if self.debug:
            print("scheduling a tool")
        # print(response_message)
        
        # Determine which tool to call
        # The agent can request multiple tool calls, but handling them in parallel in our current architecture requires 
        # mapping them to specific tool instances.
        
        # Notify user of tool usage
        tool_names = [tc.function.name for tc in response_message.tool_calls]
        print(f"[Agent is calling tools: {', '.join(tool_names)}]")
        
        tool_instances_to_call = []
        
        for tool_call in response_message.tool_calls:
            function_name = tool_call.function.name
            
            found_tool = next((t for t in self.tools if t.schema["function"]["name"] == function_name), None)
            
            if found_tool:
                # Wrap the tool to bind it to this specific tool_call (id, args)
                # This avoids the buggy Tool.__call__ that looks at state[-1]
                tool_instances_to_call.append(ToolWrapper(found_tool, tool_call))
            else:
               print(f"ERROR: Agent requested unknown tool {function_name}")

        if not tool_instances_to_call:
             return state, None
             
        sequence_steps = list(tool_instances_to_call)
        sequence_steps.append(self)
        
        return (state, Sequence(*sequence_steps))


class ToolWrapper:
    def __init__(self, tool, tool_call):
        self.tool = tool
        self.tool_call = tool_call

    def __call__(self, state):
        import json
        
        # Extract arguments from the bound tool_call object
        # tool_call is a litellm/OpenAI object with .function.arguments (string) and .id
        fc = self.tool_call.function
        args = json.loads(fc.arguments)
        
        print(f"[System] Calling tool: {fc.name}")
        
        # Execute the tool
        tool_result = self.tool.execute(**args)
        
        # Append result to state
        tool_message = {
            "role": "tool",
            "tool_call_id": self.tool_call.id,
            "name": fc.name,
            "content": tool_result,
        }
        state["messages"].append(tool_message)
        return state, None

    @property
    def __name__(self):
        return self.__class__.__name__


class PersistableAgent(AgentWithTools):
    def __init__(self, model, tools, history, return_node=None, base_url=None, debug=False):
        super().__init__(model, tools, base_url=base_url, debug=debug)
        self.history = history
        self.return_node = return_node
        
    def __call__(self, state):
        # Run the original agent logic
        new_state, plan = super().__call__(state)
        
        # Sync the NEW messages from state back to history
        self.history.set_messages(new_state["messages"])
        
        # If no internal plan (agent done), go to return_node if specified
        if plan is None and self.return_node:
            return new_state, self.return_node
        
        return new_state, plan
