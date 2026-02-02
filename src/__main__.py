import logging
import json
from dotenv import load_dotenv

from .agent import AgentWithTools
from .flow import Node, Parallel, Sequence
from .tools import TavilySearchTool
from .runtime import Runtime, END

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

def A(state):
    logger.info("we are in A")
    return {"from_a": "A"}, None

def B(state):
    logger.info("we are in B")
    return {"from_b": "B"}, None

def HEAD(state):
    logger.info("we are in head")
    return ({}, Parallel(A, B))

# Example usage:
if __name__ == "__main__":
    load_dotenv()
    runtime = Runtime()

    # Start a new session
    tool = TavilySearchTool()
    agent = AgentWithTools("gpt-5-nano", [tool])
    initial_state = {
        "messages": [{"role": "user", "content": "what's the weather in Tokyo?"}]
    }
    session_id = runtime.start_session(agent, initial_state)
    logger.info("🚀 Started session: %s", session_id)
    runtime.run()
