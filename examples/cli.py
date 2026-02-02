import asyncio
import os
import sys
from pathlib import Path

# Add src to path so we can import modules
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dotenv import load_dotenv
from entourage.runtime import Runtime
from entourage.agent import AgentWithTools
from entourage.tools import TavilySearchTool, MemoryTool
from entourage.memory import ChatHistory, MemoryDB
from entourage.flow import Sequence

# Configuration
class PersonaConfig:
    def __init__(self, agent_name: str, user_name: str, persona_template: str, guidelines: str):
        self.agent_name = agent_name
        self.user_name = user_name
        self.persona_template = persona_template
        self.guidelines = guidelines

guidelines = """
You are a conversational AI. Follow all instructions below precisely.
---
### CORE INSTRUCTIONS [EN]
- **Primary Goal:** Act as a helpful and wise conversational partner based on the persona defined below.
- **CRITICAL RULE:** You must NEVER break character.
- **Safety:** Decline any harmful or inappropriate requests.
- You have access to a set of tools to help you perform tasks and answer questions.
- Use your tools when you need to fetch external information or perform specific tasks like remembering user details.
- After using a tool, it is critical that you proceed to fully address the user's original request, synthesizing the tool's output into your final answer. Do not get distracted by the tool-use process.
"""

name = "Jarvis"
persona = "You are Jarvis, a helpful assistant to Sasha."
APP_CONFIG = PersonaConfig(
    agent_name=name,
    user_name="Aleksandr",
    persona_template=persona,
    guidelines=guidelines,
)

# Nodes
class UserInputNode:
    def __init__(self, config: PersonaConfig, history: ChatHistory, memory_db: MemoryDB, agent_node):
        self.config = config
        self.history = history
        self.memory_db = memory_db
        self.agent_node = agent_node # Circular dependency handling in flow construction would be better, but this works for simple loop

    def __call__(self, state):
        while True:
            try:
                print("> ", end="")
                user_msg = input()
            except EOFError:
                return state, None # End session
            
            if not user_msg:
                continue
                
            if user_msg.strip() == "/new":
                new_id = self.history.start_new_session()
                print(f"[Started new chat {new_id}]")
                # clear state messages so the agent starts fresh
                state["messages"] = []
                continue
            
            # Construct the full system prompt if it's a fresh start or ensure it's there
            current_messages = self.history.get_messages()
            if not current_messages or current_messages[0].get("role") != "system":
                # Build system prompt
                persona_prompt = self.config.persona_template.format(
                    agent_name=self.config.agent_name, user_name=self.config.user_name
                )
                memories = self.memory_db.get_all()
                memory_prompt_part = ""
                if memories:
                    facts = "\n".join(f"- {fact.split('] ')[1]}" for fact in memories if "] " in fact)
                    memory_prompt_part = (
                        f"\n\nHere are facts you remember about {self.config.user_name}:\n"
                        + facts
                    )
                
                final_system_prompt = f"{persona_prompt}\n\n{self.config.guidelines}{memory_prompt_part}".strip()
                
                # Prepend system message
                updated_messages = [{"role": "system", "content": final_system_prompt}] + current_messages
                self.history.set_messages(updated_messages)

            # Add user message to history
            self.history.append({"role": "user", "content": user_msg})
            
            # Update state with latest messages
            state["messages"] = self.history.get_messages()

            # Pass control to agent
            return state, self.agent_node

    @property
    def __name__(self):
        return "UserInput"

if __name__ == "__main__":
    load_dotenv()
    
    import argparse
    import logging
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true", help="Enable debug output and graph logging")
    args = parser.parse_args()

    # Configure logging
    log_level = logging.INFO if args.debug else logging.WARNING
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    
    # Setup Paths
    agent_name_lower = APP_CONFIG.agent_name.lower()
    agent_base_dir = Path(os.path.expanduser(f"~/.entourage/{agent_name_lower}"))
    agent_base_dir.mkdir(parents=True, exist_ok=True)
    
    chat_dir = agent_base_dir / "chats"
    chat_dir.mkdir(parents=True, exist_ok=True)
    
    memory_path = agent_base_dir / "memory.txt"
    memory_db = MemoryDB(memory_path)
    
    # Load latest chat or create new
    # Simple logic: just grab latest json or make new
    chat_files = list(chat_dir.glob("*.json"))
    if chat_files:
        latest_file = max(chat_files, key=lambda f: f.stat().st_mtime)
        chat_id = latest_file.stem
        history = ChatHistory(chat_id, chat_dir)
        print(f"[Loaded chat {chat_id}]")
        # Print last few messages
        for msg in history.get_messages()[-2:]:
             content = msg.get('content')
             if msg['role'] == 'tool' and not args.debug:
                 content = "[Tool Output Result - Hidden]"
             elif msg['role'] == 'assistant' and msg.get('tool_calls') and not args.debug:
                 content = f"[Tool Call: {msg['tool_calls'][0]['function']['name']}]"
             
             print(f"{msg['role']}: {content}")
    else:
        import uuid
        chat_id = str(uuid.uuid4())
        history = ChatHistory(chat_id, chat_dir)
        print(f"[Started new chat {chat_id}]")

    # Initialize Tools
    tools = [TavilySearchTool(), MemoryTool(memory_db)]

    # Initialize implementation nodes
    # We need a wrapper for AgentWithTools to sync back to history!
    # ... (existing comments) ...
    
    # Let's subclass AgentWithTools to make it 'PersistableAgent'
    class PersistableAgent(AgentWithTools):
        def __init__(self, model, tools, history, debug=False):
            super().__init__(model, tools, debug=debug)
            self.history = history
            
        def __call__(self, state):
            # Run the original agent logic
            new_state, plan = super().__call__(state)
            
            # Sync the NEW messages from state back to history
            self.history.set_messages(new_state["messages"])
            
            if plan is None:
                return new_state, user_input_node
            
            # If there is a plan (e.g. tool execution), we return that plan.
            return new_state, plan

    agent_node = PersistableAgent("gpt-5-mini-2025-08-07", tools, history, debug=args.debug)
    user_input_node = UserInputNode(APP_CONFIG, history, memory_db, agent_node)
    
    runtime = Runtime(debug=args.debug)
    # We start with User Input
    runtime.start_session(user_input_node, {"messages": []})
    runtime.run()
