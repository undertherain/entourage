# Entourage: Control-by-Return for Autonomous Agents

Entourage is a framework for building resilient, scalable, and dynamic AI agents. It shifts the paradigm from the traditional "loop-and-call" model to a declarative **"Control-by-Return"** architecture.

> **Philosophy**: Instead of an agent holding the execution thread, nodes in Entourage are **Pure Functions**. They take a state and return a *Plan* (a data structure representing the next steps). The runtime manages the execution, ensuring fault tolerance and observability.

## Key Features

- **Control-by-Return**: Decouple logic from execution. Nodes return declarative plans (`Sequence`, `Parallel`) instead of calling functions directly.
- **Dynamic Graph Generation**: The execution graph is built and modified on the fly based on agent decisions, allowing for highly adaptive workflows.
- **Stateless & Resilient**: Because state is explicitly passed and returned, workers can be fully stateless. Execution can be paused, persisted, and resumed on any machine ("dehydration/rehydration").
- **Time Travel Debugging**: Since the control flow is a persisted data structure, you can replay sessions, inspect past states, and debug failures with precision.

## Installation

Entourage is available as a Python package.

```bash
git clone https://github.com/your-repo/entourage.git
cd entourage
pip install -e .
```

## Quick Start

We provide a CLI example that demonstrates a Persistable Agent with memory and search tools.

### 1. Configure Environment
Create a `.env` file with your API keys:
```bash
TAVILY_API_KEY=tvly-...
OPENAI_API_KEY=sk-...
```

### 2. Run the CLI
```bash
python3 examples/cli.py
```

### 3. Usage
- Interact naturally with the agent.
- Use `/new` to start a fresh session (clears context but keeps long-term memory).
- Use `--debug` flag for verbose output and graph generation.

```bash
python3 examples/cli.py --debug
```

## Architecture

### The Execution Graph
Every step in an agent's lifecycle is a node in a Directed Acyclic Graph (DAG).
- **Nodes**: Pure functions `f(state) -> (new_state, Plan)`.
- **Edges**: Dependencies between executions.
- **Plans**: Data structures (`Sequence`, `Parallel`) that tell the runtime how to modify the graph.

### Example: A "Reason-Act" Node
```python
def my_reasoning_node(state):
    # Logic to determine next step...
    if needs_search:
        return state, Sequence(SearchTool(query="..."), my_reasoning_node)
    
    return state, None # Done
```

This simple return value drives the entire orchestration, allowing the system to handle the complexities of scheduling, retries, and state persistence.

## Contributing

We welcome contributions! Please see `CONTRIBUTING.md` for details.
