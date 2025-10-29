# from collections import deque
import logging
import queue
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Set, Tuple, Union

from dotenv import load_dotenv
from graphviz import Digraph
from openai import OpenAI

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
from tools import TavilySearchTool

Node = Callable[
    [Union[Dict[str, Any], List[Dict[str, Any]]]], Tuple[Dict[str, Any], Any]
]  # Node: (state) -> (new_state, schedule)
Schedule = Any  # Will be Sequence, Parallel, or Node


class ControlFlow:
    pass


class Sequence(ControlFlow):
    """Represents a sequential chain of nodes or sub-schedules."""

    def __init__(self, *items: Union[Node, "Schedule"]):
        self.items = items

    def __repr__(self):
        return f"Sequence of {self.items}"


class Parallel(ControlFlow):
    """Represents a fork-join: execute items in parallel, join results."""

    def __init__(self, *items: Union[Node, "Schedule"]):
        self.items = items

    def __repr__(self):
        return f"Parallel execution of {self.items}"


# MEMO: add support for extensions like
# .with_timeout(10)
# .with_merge(my merge)


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

        print("Agent with tools, got state:", state)
        if "tool_call_result" in state:
            print("got fake tool result, exiting")
            return state, None
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
            print("returning state:")
            print(state)
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


class Tool:
    def __call__(self, state):
        print("simulating tool call, got state", state)
        tool_message = {
            "role": "tool",
            "tool_call_id": 1,  # tool_call.id,
            "name": "dummy function",  # function_name,
            "content": "function_response",
        }
        state["messages"].append(tool_message)
        return state, None

    @property
    def __name__(self):
        return self.__class__.__name__


def default_merge(state: List[Dict[str, Any]]) -> Tuple[Dict[str, Any], None]:
    logger.info("we are in MERGE with states: %s", state)
    merged_state = {}
    if isinstance(state, list):
        for s in state:
            merged_state.update(s)
    return merged_state, None


def A(state: Dict[str, Any]) -> Tuple[Dict[str, str], None]:
    logger.info("we are in A")
    # return Parallel(X, Y)
    return {"from_a": "A"}, None


def B(state: Dict[str, Any]) -> Tuple[Dict[str, str], None]:
    logger.info("we are in B")
    return {"from_b": "B"}, None


def HEAD(
    state: Dict[str, Any],
) -> Tuple[Dict[str, Any], Union[Sequence, Parallel]]:
    logger.info("we are in head")
    # return {}, A
    return ({}, Sequence(A, B))
    # return ({}, Parallel(A, B))


class EndObject:
    """Special object to represent the end of execution."""

    def __str__(self):
        return "END"

    def __repr__(self):
        return "END"


END = EndObject()


@dataclass
class Execution:
    # node: Union[Callable, ControlFlow]
    node: Callable
    exec_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    nodes_in: Set[str] = field(default_factory=set)
    nodes_out: Set[str] = field(default_factory=set)
    done: bool = False

    def __call__(self, state: Union[Dict[str, Any], List[Dict[str, Any]]]):
        return self.node(state)


@dataclass
class Session:
    """Represents a single execution session with its own graph and state."""

    session_id: str
    graph: Dict[str, Execution] = field(default_factory=dict)
    states: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    completed: bool = False

    def __post_init__(self):
        if not self.session_id:
            self.session_id = str(uuid.uuid4())

    def add_node(self, node: Execution):
        """Add a node to the session's execution graph."""
        self.graph[node.exec_id] = node

    def add_edge(self, start: str, end: str):
        """Add an edge between two nodes in the session's execution graph."""
        self.graph[start].nodes_out.add(end)
        self.graph[end].nodes_in.add(start)


class Runtime:
    def __init__(self):
        self.ready_queue = queue.Queue()
        self.sessions: Dict[str, Session] = {}

    def parse_schedule(self, schedule, session: Session):
        logger.info("adding schedule %s", schedule)
        if isinstance(schedule, Sequence):
            executions = []
            for node in schedule.items:
                starts, end = self.parse_schedule(node, session)
                if executions:
                    for start in starts:
                        session.add_edge(executions[-1], start)
                executions.append(starts)
                executions.append(end)
            return executions[0], executions[-1]
        elif isinstance(schedule, Parallel):
            merge = Execution(default_merge)
            session.add_node(merge)
            starts = []
            for node in schedule.items:
                new_starts, end = self.parse_schedule(node, session)
                session.add_edge(end, merge.exec_id)
                for start in new_starts:
                    starts.append(start)
            return starts, merge.exec_id
        else:
            # single node
            execution = Execution(schedule)
            session.add_node(execution)
            return [execution.exec_id], execution.exec_id

    def get_nodes_in_executions(self, node_id: str, session: Session):
        in_ids = session.graph[node_id].nodes_in
        return [session.graph[i] for i in in_ids]

    def execute_node(self, execution_id: str, session_id: str):
        session = self.sessions[session_id]
        execution = session.graph[execution_id]

        if execution.node == END:
            logger.info("🏁 Session %s is done!!", session_id)
            # self.visualize_graph(session)
            session.completed = True
            self.cleanup_session(session_id)
            return

        # Get input states from parent nodes
        print("collecting inputs from ", execution.nodes_in)
        input_states = [
            session.states[in_id]
            for in_id in execution.nodes_in
            if in_id in session.states
        ]
        if len(input_states) == 1:
            state = input_states[0]
        elif len(input_states) > 1:
            state = input_states
        else:
            state = {}

        # Execute the node
        new_state, schedule = execution(state)
        execution.done = True

        # Store the output state for this execution
        if new_state is not None:
            # print("saving returned state for ", execution_id, "as", new_state)
            session.states[execution_id] = new_state

        if schedule:
            original_next = set(execution.nodes_out)
            # print(original_next)
            starts, end = self.parse_schedule(schedule, session)
            session.graph[end].nodes_out = original_next
            for i in list(original_next):
                session.graph[i].nodes_in.remove(execution_id)
                session.graph[i].nodes_in.add(end)
            for i in starts:
                session.graph[i].nodes_in = {execution_id}
            execution.nodes_out = starts

            for start in starts:
                self.ready_queue.put((start, session_id))
        else:
            for next_id in execution.nodes_out:
                ready = True
                for dep in self.get_nodes_in_executions(next_id, session):
                    if not dep.done:
                        ready = False
                        break

                if ready:
                    self.ready_queue.put((next_id, session_id))

    def start_session(
        self, initial_node: Node, initial_state: Dict[str, Any] = None
    ) -> str:
        """Start a new session with the given initial node and state."""
        session = Session(session_id=str(uuid.uuid4()))
        initial_execution = Execution(initial_node)
        if initial_state is None:
            initial_state = {}
        initial_execution.nodes_in = ["initial"]
        session.states["initial"] = initial_state

        end_execution = Execution(END)

        session.add_node(initial_execution)
        session.add_node(end_execution)
        session.add_edge(initial_execution.exec_id, end_execution.exec_id)

        self.sessions[session.session_id] = session
        self.ready_queue.put((initial_execution.exec_id, session.session_id))

        return session.session_id

    def cleanup_session(self, session_id: str):
        """Clean up a completed session."""
        if session_id in self.sessions:
            del self.sessions[session_id]
        logger.info("Cleaned up session %s", session_id)

    def visualize_graph(self, session: Session):
        """Visualize the execution graph for a specific session."""
        dot = Digraph(comment=f"Execution Graph - Session {session.session_id[:8]}")

        for exec_id, execution in session.graph.items():
            node_name = (
                execution.node.__name__
                if callable(execution.node)
                else str(execution.node)
            )
            dot.node(exec_id, f"{node_name}\n({exec_id[:8]})")

        for exec_id, execution in session.graph.items():
            for out_id in execution.nodes_out:
                dot.edge(exec_id, out_id)

        filename = f"execution_graph_{session.session_id[:8]}"
        dot.render(filename, view=True)

    def run(self):
        """Main runtime loop that processes ready nodes from all sessions."""
        while True:
            try:
                execution_id, session_id = self.ready_queue.get(timeout=1.0)
                if (
                    session_id in self.sessions
                    and not self.sessions[session_id].completed
                ):
                    logger.info(
                        "Executing node %s for session %s", execution_id, session_id
                    )
                    self.execute_node(execution_id, session_id)
            except queue.Empty:
                # Check if all sessions are completed
                if not self.sessions:
                    logger.info("All sessions completed. Runtime shutting down.")
                    break
                continue


# Example usage:
if __name__ == "__main__":
    load_dotenv()
    runtime = Runtime()

    # Start a new session
    # tool = Tool()
    tool = TavilySearchTool()
    agent = AgentWithTools("gpt-5-nano", [tool])
    initial_state = {
        "messages": [{"role": "user", "content": "what's the weather in Tokyo?"}]
    }
    session_id = runtime.start_session(agent, initial_state)
    logger.info("🚀 Started session: %s", session_id)

    # Run the runtime
    runtime.run()
