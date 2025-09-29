# from collections import deque
import queue
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Set, Tuple, Union

# Type aliases for clarity
Node = Callable[
    [Dict[str, Any]], Tuple[Dict[str, Any], Any]
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


# .with_timeout(10)
# .with_merge(my merge)


def X():
    pass


def Y():
    pass


def MERGE():
    pass


def A():
    print("we are in A")
    # return Parallel(X, Y)
    return None, None


def B():
    print("we are in B")
    return None, None


def HEAD():
    print("we are in head")
    # return ({}, Sequence(A, B))
    return ({}, Sequence(A, A, B))


END = object()


@dataclass
class Execution:
    # node: Union[Callable, ControlFlow]
    node: Callable
    exec_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    nodes_in: Set[str] = field(default_factory=set)
    nodes_out: Set[str] = field(default_factory=set)

    def __call__(self):
        return self.node()


# class Graph:
#     def __init__(self):
#         self.nodes = {"A"}
#         self.edges = {"}


class Runtime:
    # TODO: fetch node from graph by ID

    def __init__(self):
        self.ready_queue = queue.Queue()
        self.graph = {}

    def parse_schedule(self, schedule):
        print("adding schedule", schedule)
        if isinstance(schedule, Sequence):
            executions = []
            for node in schedule.items:
                start, end = self.parse_schedule(node)
                if executions:
                    self.add_edge(executions[-1], start)
                executions.append(start)
                executions.append(end)
            return executions[0], executions[-1]
        else:
            # single node
            execution = Execution(schedule)
            self.add_node(execution)
            return execution.exec_id, execution.exec_id
            # self.graph[execution.exec_id] = execution
            # return execution.exec_id, execution.exec_id

        # in add all nodes to the graph
        # sever old connections
        # make a copy of the state in case multple

    def execute_node(self, execution_id):
        execution = self.graph[execution_id]
        if execution.node == END:
            print("this session is done!!")
            return
        original_next = self.graph[execution.nodes_out.pop()]
        original_next.nodes_in.remove(execution_id)
        # if it's Parallel, just update queue
        # if isinstance(execution, Parallel):
        # ok we know that node finished
        state, schedule = execution()
        if schedule:
            assert len(execution.nodes_out) == 0
            start, end = self.parse_schedule(schedule)
            print(start, end)
            last_out = execution.nodes_out
            execution.nodes_out = start
            self.add_edge(end, original_next.exec_id)
            # original_next.nodes_in.add(end)
            self.ready_queue.put(start)
            # old_next = execution.
            # if Parallel: create branches
            # if isinstance(start.node, Parallel):
            # for actual_node in start.node.items:
            # self.ready_queue.put(actual_node)
        else:
            if not original_next.nodes_in:
                self.ready_queue.put(original_next.exec_id)
        # pop current node
        # if not isinstance(next_steps, tuple):
        #     next_steps = (next_steps,)
        # for i in next_steps:
        #     q.put(i)

    def generate_id(self) -> str:
        return str(uuid.uuid4())[:8]  # Short UUID for demo

    def crate_session_terminal(self, start):
        return {
            "node": End,
            "inputs": [start],
        }

    def add_edge(self, start, end):
        self.graph[start].nodes_out.add(end)
        self.graph[end].nodes_in.add(start)

    def add_node(self, node):
        self.graph[node.exec_id] = node

    def add_initial(self, initial_node):
        initial_execution = Execution(initial_node)
        end_execution = Execution(END)
        self.add_node(initial_execution)
        self.add_node(end_execution)
        self.add_edge(initial_execution.exec_id, end_execution.exec_id)
        self.ready_queue.put(initial_execution.exec_id)
        # ["state"] = initial_state
        # create endge from initial to edge

    def run(self, initial_state, initial_node):
        self.add_initial(initial_node)
        while True:
            next_node = self.ready_queue.get()
            print("@main loop next node", next_node)
            print("executing", next_node, self.graph[next_node].node)
            self.execute_node(next_node)


runtime = Runtime()
runtime.run({}, HEAD)
