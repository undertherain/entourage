# General Python Coding Standards

1.  **Code Style**: Adhere to PEP 8 standards. Use snake_case for variable and function names. All functions must include type hints for arguments and return values.
2.  **Filesystem Operations**: Always use the `pathlib` library for all filesystem paths and I/O operations. Do not use the `os` module for path manipulation.
3.  **API Keys & Secrets**: Assume all API keys are stored in a `.env` file at the project root. Use the `python-dotenv` library's `load_dotenv()` function to load them. Never hardcode secrets.
4.  **LLM Integration**: When generating code that interacts with a large language model, use the official OpenAI SDK and default to the `gpt-5-nano` model.
5.  **Commenting**: Comments should be high-level and explain architectural decisions or the "why" behind complex logic. Do not add obvious comments that explain "what" the code is doing (e.g., `# opening a file`).
6.  **Docstrings**: All public functions and classes must have a Google-style docstring.
