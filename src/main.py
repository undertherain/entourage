# from collections import deque
import queue
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Set, Tuple, Union

from graphviz import Digraph

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

    def __repr__(self):
        return f"Parallel execution of {self.items}"


# .with_timeout(10)
# .with_merge(my merge)


def X():
    pass


def Y():
    pass


def default_merge():
    print("we are in MERGE")
    return None, None


def A():
    print("we are in A")
    # return Parallel(X, Y)
    return None, None


def B():
    print("we are in B")
    return None, None


def HEAD():
    print("we are in head")
    # return {}, A
    # return ({}, Sequence(A, B))
    return ({}, Parallel(A, B))


END = object()


@dataclass
class Execution:
    # node: Union[Callable, ControlFlow]
    node: Callable
    exec_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    nodes_in: Set[str] = field(default_factory=set)
    nodes_out: Set[str] = field(default_factory=set)
    done: bool = False

    def __call__(self):
        return self.node()


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
                starts, end = self.parse_schedule(node)
                if executions:
                    for start in starts:
                        self.add_edge(executions[-1], start)
                executions.append(starts)
                executions.append(end)
            return executions[0], executions[-1]
        elif isinstance(schedule, Parallel):
            merge = Execution(default_merge)
            self.add_node(merge)
            starts = []
            for node in schedule.items:
                new_starts, end = self.parse_schedule(node)
                # self.add_edge(current, start)
                self.add_edge(end, merge.exec_id)
                for start in new_starts:
                    # print("addign node from end of parallel child to merge", start, end)
                    starts.append(start)
            return starts, merge.exec_id
        else:
            # single node
            execution = Execution(schedule)
            self.add_node(execution)
            return [execution.exec_id], execution.exec_id

    def execute_node(self, execution_id):
        execution = self.graph[execution_id]
        if execution.node == END:
            print("this session is done!!")
            self.visualize_graph()
            return
        # original_next.nodes_in.remove(execution_id)
        # if it's Parallel, just update queue
        # if isinstance(execution, Parallel):
        # ok we know that node finished
        state, schedule = execution()
        execution.done = True
        if schedule:
            # assert len(execution.nodes_out) == 0
            original_next = set(execution.nodes_out)
            print("original next", original_next)
            starts, end = self.parse_schedule(schedule)
            self.graph[end].nodes_out = original_next
            execution.nodes_out = starts
            print("returned schedule", starts, end)
            for start in starts:
                self.ready_queue.put(start)
        else:
            for next_id in execution.nodes_out:
                # TODO: check if all dependencies are met
                print(
                    f"checking if\n\t{next_id}\n\t{self.graph[next_id]}\n\tis ready\n"
                )
                ready = True
                for dep in self.get_nodes_in_executions(next_id):
                    if not dep.done:
                        ready = False
                        print("NO")
                if ready:
                    print("YES")
                    self.ready_queue.put(next_id)

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

    def get_nodes_in_executions(self, node_id):
        in_ids = self.graph[node_id].nodes_in
        return [self.graph[i] for i in in_ids]

    def visualize_graph(self):
        dot = Digraph(comment="Execution Graph")
        for exec_id, execution in self.graph.items():
            node_name = (
                execution.node.__name__
                if callable(execution.node)
                else str(execution.node)
            )
            dot.node(exec_id, f"{node_name}\n({exec_id[:8]})")

        for exec_id, execution in self.graph.items():
            for out_id in execution.nodes_out:
                dot.edge(exec_id, out_id)

        dot.render("execution_graph", view=True)

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
