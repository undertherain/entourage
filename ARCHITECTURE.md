Entourage is an SDK for developing agentic systems based on following principles:

- system consists of stateless executable nodes (similar to LangGraph's approach), between which a state object is passed by the runtime.

- unlike LangGraph, however, the execution graph does not have to be pre-defined

- at session start, initial graph with just one edge is created from HEAD node and END marker.

- any node can return an execution plan that will be injected in the graph between this node and the node that was originally following it.

- execution plan can contain individaul nodes, sequences of nodes (Sequence class) or fork-joins (Parallel class), all of it will be handled by the runtime transparently.

- states, execution graph, ready to execute queue are persisten in some sort of global storage to achieve fault-tolerance.

- system support multiple storage backends, e.g. local in-memory for debug, AWS for deployment.

