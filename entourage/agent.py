from litellm import completion

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
            # Look up the tool executable from available tools 
            # We need the ACTUAL tool instance or a wrapper that knows how to execute it since 
            # our `self.tools` list contains instances.
            
            # We need to find the tool instance that matches this name.
            # Ideally `self.available_tools` would store the instance itself, not just the execute method 
            # IF our tool execution logic (Sequence) expects a callable that takes state.
            
            # Our `Tool` class in `entourage/tools.py` has a `__call__` method that handles state processing.
            # So if we pass the TOOL INSTANCE to Sequence, it will work.
            
            found_tool = next((t for t in self.tools if t.schema["function"]["name"] == function_name), None)
            
            if found_tool:
                tool_instances_to_call.append(found_tool)
            else:
               print(f"ERROR: Agent requested unknown tool {function_name}")

        if not tool_instances_to_call:
             return state, None
             
        # If multiple tools, we could return Parallel(tool_instances_to_call) if we implemented a merge.
        # For now, let's just chain them or take the first one if we want to be safe for now, 
        # or just handle the first one. 
        # The original code did `Sequence(self.tools[0], self)`.
        
        # Let's do a Sequence of the requested tool + return to Self.
        # If there are multiple, we should probably do Sequence(t1, t2, ..., self)
        
        sequence_steps = list(tool_instances_to_call)
        sequence_steps.append(self)
        
        return (state, Sequence(*sequence_steps))

    @property
    def __name__(self):
        return self.__class__.__name__
