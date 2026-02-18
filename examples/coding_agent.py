import os
import sys
from pathlib import Path

# Add src to path so we can import modules
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from dotenv import load_dotenv
from entourage.runtime import Runtime
from entourage.agent import AgentWithTools, PersistableAgent
from entourage.tools import ListDirTool, ReadFileTool, WriteFileTool
from entourage.memory import ChatHistory
from entourage.flow import Sequence

# Configuration
class PersonaConfig:
    def __init__(self, agent_name: str, user_name: str, persona_template: str, guidelines: str):
        self.agent_name = agent_name
        self.user_name = user_name
        self.persona_template = persona_template
        self.guidelines = guidelines

guidelines = """
You are an expert software engineer AI. Follow all instructions below precisely.
---
### CORE INSTRUCTIONS [EN]
- **Primary Goal:** Assist the user with coding tasks, debugging, and explaining code.
- **Tools:**
  - `list_files`: Use this to explore the directory structure.
  - `read_file`: Use this to read the content of files.
  - `write_file`: Use this to write content to a file. Behavior: overwritten if exists, created if not.
- **Analysis:** proper analysis often requires understanding the project structure first, then reading specific files.
- **Safety:** You have write access. Be careful when overwriting files. Always double-check path.
- **Communication:** Be concise and technical.
"""

name = "CoderBot"
persona = "You are CoderBot, an expert coding assistant."
APP_CONFIG = PersonaConfig(
    agent_name=name,
    user_name="Developer",
    persona_template=persona,
    guidelines=guidelines,
)

# Nodes
class UserInputNode:
    def __init__(self, config: PersonaConfig, history: ChatHistory, agent_node):
        self.config = config
        self.history = history
        self.agent_node = agent_node

    def __call__(self, state):
        while True:
            try:
                print("> ", end="")
                user_msg = input()
            except EOFError:
                return state, None 
            
            if not user_msg:
                continue
                
            if user_msg.strip() == "/new":
                new_id = self.history.start_new_session()
                print(f"[Started new chat {new_id}]")
                state["messages"] = []
                continue
            
            # Construct system prompt if needed
            current_messages = self.history.get_messages()
            if not current_messages or current_messages[0].get("role") != "system":
                final_system_prompt = f"{self.config.persona_template}\n\n{self.config.guidelines}".strip()
                updated_messages = [{"role": "system", "content": final_system_prompt}] + current_messages
                self.history.set_messages(updated_messages)

            # Add user message
            self.history.append({"role": "user", "content": user_msg})
            
            # Update state with latest messages
            state["messages"] = self.history.get_messages()

            return state, self.agent_node

    @property
    def __name__(self):
        return "UserInput"

if __name__ == "__main__":
    load_dotenv()
    
    import argparse
    import logging
    parser = argparse.ArgumentParser()
    parser.add_argument("--model", type=str, default="claude-3-haiku-20240307", help="Model name to use")
    parser.add_argument("--base-url", type=str, default=None, help="Base URL for the API")
    parser.add_argument("--debug", action="store_true", help="Enable debug output")
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
    
    # Load latest chat or create new
    chat_files = list(chat_dir.glob("*.json"))
    if chat_files:
        latest_file = max(chat_files, key=lambda f: f.stat().st_mtime)
        chat_id = latest_file.stem
        history = ChatHistory(chat_id, chat_dir)
        print(f"[Loaded chat {chat_id}]")
        for msg in history.get_messages()[-10:]:
             content = msg.get('content')
             role = msg.get('role')
             if role == 'system': continue
             if role == 'tool' and not args.debug: content = "[Tool Output Result - Hidden]"
             elif role == 'assistant':
                 if msg.get('tool_calls') and not args.debug:
                     content = f"[Tool Call: {msg['tool_calls'][0]['function']['name']}]"
                 elif not content:
                      if msg.get('function_call'): content = f"[Function Call: {msg['function_call'].get('name')}]"
                      else: content = "" 
             if content: print(f"{role}: {content}")
    else:
        import uuid
        chat_id = str(uuid.uuid4())
        history = ChatHistory(chat_id, chat_dir)
        print(f"[Started new chat {chat_id}]")

    # Initialize Tools
    tools = [ListDirTool(), ReadFileTool(), WriteFileTool()]

    # PersistableAgent is now imported from entourage.agent
    
    agent_node = PersistableAgent(args.model, tools, history, return_node=None, base_url=args.base_url, debug=args.debug)
    user_input_node = UserInputNode(APP_CONFIG, history, agent_node)
    
    # Wire the return node
    agent_node.return_node = user_input_node
    
    runtime = Runtime(debug=args.debug)
    runtime.start_session(user_input_node, {"messages": []})
    runtime.run()
